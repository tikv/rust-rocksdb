// Copyright 2025 Contributors to rust-rocksdb
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This example demonstrates how to use the range-based SST file metadata API
// to get information about SST files that contain data within a specific key range.

use rocksdb::{DBOptions, FlushOptions, SstFileInfo, Writable, DB};
use std::env;
use std::path::Path;

fn print_file_info(file: &SstFileInfo, index: usize) {
    let smallest_str = String::from_utf8_lossy(&file.smallest_key);
    let largest_str = String::from_utf8_lossy(&file.largest_key);
    
    println!(
        "  File {}: {} (Level {}, {} bytes, keys: {}..{}, entries: {}, deletions: {})",
        index + 1,
        file.name,
        file.level,
        file.size,
        smallest_str,
        largest_str,
        file.num_entries,
        file.num_deletions
    );
}

fn analyze_range_files(db: &DB, start_key: Option<&[u8]>, end_key: Option<&[u8]>, description: &str) {
    println!("\n=== {} ===", description);
    
    match db.get_sst_files_in_range_default(start_key, end_key) {
        Ok(files) => {
            if files.is_empty() {
                println!("No SST files found in the specified range.");
            } else {
                println!("Found {} SST file(s) in range:", files.len());
                for (i, file) in files.iter().enumerate() {
                    print_file_info(file, i);
                }
                
                // Calculate total size and statistics
                let total_size: usize = files.iter().map(|f| f.size).sum();
                let total_entries: u64 = files.iter().map(|f| f.num_entries).sum();
                let total_deletions: u64 = files.iter().map(|f| f.num_deletions).sum();
                println!("Total size: {} bytes", total_size);
                println!("Total entries: {}", total_entries);
                println!("Total deletions: {}", total_deletions);
                println!("Deletion ratio: {:.2}%", 
                    if total_entries > 0 { (total_deletions as f64 / total_entries as f64) * 100.0 } else { 0.0 });
                
                // Show level distribution
                let mut level_counts = std::collections::HashMap::new();
                for file in &files {
                    *level_counts.entry(file.level).or_insert(0) += 1;
                }
                println!("Files by level: {:?}", level_counts);
            }
        }
        Err(e) => {
            println!("Error getting files in range: {}", e);
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get the database path from command line arguments or use a default
    let db_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/rocksdb_sst_file_info_example".to_string());

    // Remove the existing database if it exists
    // Note: In a real application, you might want to handle cleanup differently

    // Open the database
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let db = DB::open_default(&db_path)?;

    println!("Database opened at: {}", db_path);

    // Insert data in different key ranges to create multiple SST files
    println!("\nInserting data in different key ranges...");
    let mut flush_opts = FlushOptions::default();
    flush_opts.set_wait(true);

    // Group 1: Keys starting with "user_"
    for i in 0..5 {
        let key = format!("user_{:03}", i);
        let value = format!("user_data_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("Inserted: {} -> {}", key, value);
    }
    db.flush(&flush_opts)?;
    println!("Flushed user data");

    // Group 2: Keys starting with "order_"
    for i in 0..5 {
        let key = format!("order_{:03}", i);
        let value = format!("order_data_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("Inserted: {} -> {}", key, value);
    }
    db.flush(&flush_opts)?;
    println!("Flushed order data");

    // Group 3: Keys starting with "product_"
    for i in 0..5 {
        let key = format!("product_{:03}", i);
        let value = format!("product_data_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("Inserted: {} -> {}", key, value);
    }
    db.flush(&flush_opts)?;
    println!("Flushed product data");

    // Now analyze different key ranges
    analyze_range_files(&db, None, None, "All SST Files");
    
    analyze_range_files(&db, Some(b"user_000"), Some(b"user_999"), "User Data Range");
    
    analyze_range_files(&db, Some(b"order_000"), Some(b"order_999"), "Order Data Range");
    
    analyze_range_files(&db, Some(b"product_000"), Some(b"product_999"), "Product Data Range");
    
    analyze_range_files(&db, Some(b"order_001"), Some(b"order_003"), "Specific Order Range");
    
    analyze_range_files(&db, Some(b"system_"), Some(b"system_999"), "Non-existent Range");

    // Demonstrate the overlap checking functionality
    println!("\n=== Overlap Analysis ===");
    match db.get_sst_files_in_range_default(Some(b"user_000"), Some(b"user_999")) {
        Ok(files) => {
            for file in &files {
                println!("\nAnalyzing file: {}", file.name);
                
                // Test different overlap scenarios
                let test_ranges = [
                    (Some(b"user_001" as &[u8]), Some(b"user_003" as &[u8]), "user_001..user_003"),
                    (Some(b"user_002" as &[u8]), Some(b"user_005" as &[u8]), "user_002..user_005"),
                    (Some(b"order_000" as &[u8]), Some(b"order_999" as &[u8]), "order_000..order_999"),
                    (Some(b"user_000" as &[u8]), Some(b"user_999" as &[u8]), "user_000..user_999"),
                ];
                
                for (start, end, desc) in &test_ranges {
                    let overlaps = file.overlaps_with_range(*start, *end);
                    let contained = file.is_contained_in_range(*start, *end);
                    println!("  Range {}: overlaps={}, contained={}", desc, overlaps, contained);
                }
            }
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }

    // Demonstrate binary search optimization
    println!("\n=== Binary Search Optimization ===");
    println!("For levels 1 and above, SST files are non-overlapping and sorted by smallest_key.");
    println!("The API uses binary search to find the first overlapping file, then iterates forward");
    println!("until it encounters a file whose smallest_key >= end_key. This provides O(log n + k)");
    println!("performance where k is the number of overlapping files, instead of O(n) for each level.");
    
    // Show performance benefit by analyzing a specific range
    analyze_range_files(&db, Some(b"order_002"), Some(b"order_004"), "Specific Order Range (Binary Search Optimized)");

    // Clean up
    drop(db);
    // Note: In a real application, you might want to handle cleanup differently
    println!("\nExample completed successfully!");

    Ok(())
}
