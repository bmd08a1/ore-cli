use std::{sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc}, time::Instant};
use std::time::Duration;

use colored::*;
use drillx::{
    equix::{self},
    Hash, Solution,
};
use ore_api::{
    consts::{BUS_ADDRESSES, BUS_COUNT, EPOCH_DURATION},
    state::{Bus, Config, Proof},
};
use ore_utils::AccountDeserialize;
use rand::Rng;
use solana_program::pubkey::Pubkey;
use solana_rpc_client::spinner;
use solana_sdk::signer::Signer;

use crate::{
    args::MineArgs,
    send_and_confirm::ComputeBudget,
    utils::{
        amount_u64_to_string, get_clock, get_config, get_updated_proof_with_authority, proof_pubkey,
    },
    Miner,
};

const MIN_MINE_TIME: u64 = 15;

impl Miner {
    pub async fn mine(&self, args: MineArgs) {
        // Open account, if needed.
        let signer = self.signer();
        self.open().await;

        // Check num threads
        self.check_num_cores(args.cores);

        // Start mining loop
        let start = Instant::now();
        let mut num_hash_created = 0;
        let mut best_difficulty_created = 0;
        let mut mining_time = 0;
        let mut total_rewards = 0;
        let mut last_rewards = 0;
        let mut last_hash_at = 0;
        let mut last_balance = 0;

        loop {
            // Fetch proof
            let config = get_config(&self.rpc_client).await;
            let proof =
                get_updated_proof_with_authority(&self.rpc_client, signer.pubkey(), last_hash_at)
                    .await;

            if last_balance != 0 {
                last_rewards = proof.balance - last_balance;
                total_rewards += last_rewards;
            }
            last_balance = proof.balance;
            if num_hash_created > 0 {
                println!("----------------------------------------------");
                println!("- Number of hash created: {} (best difficulty: {})", num_hash_created, best_difficulty_created);
                println!(
                    "- Time elapsed: {} (Mining: {}, submitting tx: {})",
                    format_duration(start.elapsed().as_secs()),
                    format_duration(mining_time),
                    format_duration(start.elapsed().as_secs() - mining_time)
                    );
                println!("- Rewards: {} ORE (last: {})", amount_u64_to_string(total_rewards), amount_u64_to_string(last_rewards));
                println!("----------------------------------------------");
            }

            println!(
                "\n\nStake: {} ORE\n  Multiplier: {:12}x",
                amount_u64_to_string(proof.balance),
                calculate_multiplier(proof.balance, config.top_balance)
            );
            last_hash_at = proof.last_hash_at;

            // Calculate cutoff time
            let cutoff_time = self.get_cutoff(proof, args.buffer_time).await;

            // Run drillx
            let miner_timer = Instant::now();
            let (solution, should_increase_fee, best_difficulty) =
                Self::find_hash_par(proof, cutoff_time, args.cores, args.min_difficulty, args.best_difficulty)
                    .await;
            mining_time += miner_timer.elapsed().as_secs();

            num_hash_created += 1;
            if best_difficulty.gt(&best_difficulty_created) {
                best_difficulty_created = best_difficulty
            }

            // Build instruction set
            let mut ixs = vec![ore_api::instruction::auth(proof_pubkey(signer.pubkey()))];
            let mut compute_budget = 500_000;
            if self.should_reset(config).await && rand::thread_rng().gen_range(0..100).eq(&0) {
                compute_budget += 100_000;
                ixs.push(ore_api::instruction::reset(signer.pubkey()));
            }

            // Build mine ix
            ixs.push(ore_api::instruction::mine(
                signer.pubkey(),
                signer.pubkey(),
                self.find_bus().await,
                solution,
            ));

            // Submit transaction
            match self.send_and_confirm(&ixs, ComputeBudget::Fixed(compute_budget), false, should_increase_fee)
                .await {
                    Ok(_) => {}
                    Err(_) => {}
                };
        }
    }

    async fn find_hash_par(
        proof: Proof,
        cutoff_time: u64,
        cores: u64,
        min_difficulty: u32,
        best: u32,
    ) -> (Solution, bool, u32) {
        // Dispatch job to each thread
        let progress_bar = Arc::new(spinner::new_progress_bar());
        let found_best_solution = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel();
        progress_bar.set_message("Mining...");

        // Producer
        let core_ids = core_affinity::get_core_ids().unwrap();
        let hashers: Vec<_> = core_ids
            .into_iter()
            .map(|i| {
                std::thread::spawn({
                    let proof = proof.clone();
                    let found_best_solution_clone = found_best_solution.clone();
                    let mut memory = equix::SolverMemory::new();
                    let tx_clone = tx.clone();

                    move || {
                        // Return if core should not be used
                        if (i.id as u64).ge(&cores) {
                            return;
                        }

                        // Pin to core
                        let _ = core_affinity::set_for_current(i);

                        // Start hashing
                        let mut nonce = u64::MAX.saturating_div(cores).saturating_mul(i.id as u64);
                        loop {
                            if found_best_solution_clone.load(Ordering::Relaxed) {
                                break;
                            }

                            // Create hash
                            if let Ok(hx_array) = drillx::hash_with_memory(
                                &mut memory,
                                &proof.challenge,
                                &nonce.to_le_bytes(),
                            ) {
                                for hx in hx_array.into_iter() {
                                    if hx.is_valid(&proof.challenge, &nonce.to_le_bytes()) {
                                        let difficulty = hx.difficulty();

                                        let _ = tx_clone.send((hx, nonce, difficulty));
                                    }
                                }
                            }

                            // Increment nonce
                            nonce += 1;
                        }
                    }
                })
            })
            .collect();

        // Consumer
        let sorter = std::thread::spawn({
            let found_best_solution_clone = found_best_solution.clone();
            let progress_bar = progress_bar.clone();

            move || {
                let mut best_nonce = 0;
                let mut best_difficulty = 0;
                let mut best_hash = Hash::default();
                let mut counter = 0;
                let timer = Instant::now();

                loop {
                    if timer.elapsed().as_secs().gt(&cutoff_time) {
                        found_best_solution_clone.store(true, Ordering::Relaxed);
                        break;
                    }

                    let (hx, nonce, difficulty): (Hash, u64, u32) = rx.recv().unwrap();

                    if difficulty.gt(&best_difficulty) {
                        best_nonce = nonce;
                        best_difficulty = difficulty;
                        best_hash = hx;
                    }

                    if best_difficulty.gt(&best) {
                        let mined_time = timer.elapsed().as_secs();

                        if mined_time < MIN_MINE_TIME {
                            std::thread::sleep(Duration::from_secs(MIN_MINE_TIME - mined_time));
                        }
                        found_best_solution_clone.store(true, Ordering::Relaxed);
                        break;
                    }

                    // Exit if time has elapsed
                    if counter % 100 == 0 {
                        progress_bar.set_message(format!(
                            "Mining... ({} sec remaining, difficulty {} / {} / {})",
                            cutoff_time.saturating_sub(timer.elapsed().as_secs()),
                            best_difficulty,
                            min_difficulty,
                            best
                        ));
                    }

                    counter += 1;
                }

                (best_hash, best_nonce, best_difficulty)
            }
        });


        for h in hashers {
            let _ = h.join();
        }
        let (best_hash, best_nonce, best_difficulty) = sorter.join().unwrap();


        // Update log
        progress_bar.finish_with_message(format!(
            "Best hash: {} (difficulty {})",
            bs58::encode(best_hash.h).into_string(),
            best_difficulty
        ));

        (Solution::new(best_hash.d, best_nonce.to_le_bytes()), best_difficulty.ge(&best), best_difficulty)
    }

    pub fn check_num_cores(&self, cores: u64) {
        let num_cores = num_cpus::get() as u64;
        if cores.gt(&num_cores) {
            println!(
                "{} Cannot exceeds available cores ({})",
                "WARNING".bold().yellow(),
                num_cores
            );
        }
    }

    async fn should_reset(&self, config: Config) -> bool {
        let clock = get_clock(&self.rpc_client).await;
        config
            .last_reset_at
            .saturating_add(EPOCH_DURATION)
            .saturating_sub(5) // Buffer
            .le(&clock.unix_timestamp)
    }

    async fn get_cutoff(&self, proof: Proof, buffer_time: u64) -> u64 {
        let clock = get_clock(&self.rpc_client).await;
        proof
            .last_hash_at
            .saturating_add(60)
            .saturating_sub(buffer_time as i64)
            .saturating_sub(clock.unix_timestamp)
            .max(0) as u64
    }

    async fn find_bus(&self) -> Pubkey {
        // Fetch the bus with the largest balance
        if let Ok(accounts) = self.rpc_client.get_multiple_accounts(&BUS_ADDRESSES).await {
            let mut top_bus_balance: u64 = 0;
            let mut top_bus = BUS_ADDRESSES[0];
            for account in accounts {
                if let Some(account) = account {
                    if let Ok(bus) = Bus::try_from_bytes(&account.data) {
                        if bus.rewards.gt(&top_bus_balance) {
                            top_bus_balance = bus.rewards;
                            top_bus = BUS_ADDRESSES[bus.id as usize];
                        }
                    }
                }
            }
            return top_bus;
        }

        // Otherwise return a random bus
        let i = rand::thread_rng().gen_range(0..BUS_COUNT);
        BUS_ADDRESSES[i]
    }
}

fn calculate_multiplier(balance: u64, top_balance: u64) -> f64 {
    1.0 + (balance as f64 / top_balance as f64).min(1.0f64)
}

fn format_duration(total_seconds: u64) -> String {
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}
