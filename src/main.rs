use std::env;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use rayon::prelude::*;
use bytecount;

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MB per chunk

/// Count newlines in a file using parallel processing without memory mapping.
fn count_newlines_in_file(file_path: &str) -> io::Result<usize> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size = metadata.len() as usize;

    if file_size == 0 {
        return Ok(0);
    }

    // Calculate the number of chunks
    let num_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // Create a vector of chunk offsets
    let chunk_offsets: Vec<usize> = (0..num_chunks)
        .map(|i| i * CHUNK_SIZE)
        .collect();

    // Use Arc to share the file path among threads
    let file_path = Arc::new(file_path.to_string());

    // Process each chunk in parallel
    let newline_count: usize = chunk_offsets.par_iter().map(|&offset| {
        let mut local_buffer = vec![0u8; CHUNK_SIZE];

        // Each thread opens its own file handle
        let mut file = File::open(&*file_path).unwrap();

        // Seek to the chunk's offset
        file.seek(SeekFrom::Start(offset as u64)).unwrap();

        // Read the chunk into the local buffer
        let bytes_to_read = CHUNK_SIZE.min(file_size - offset);
        let mut total_read = 0;
        while total_read < bytes_to_read {
            let bytes_read = file.read(&mut local_buffer[total_read..bytes_to_read]).unwrap();
            if bytes_read == 0 {
                break;
            }
            total_read += bytes_read;
        }

        // Count newlines in the buffer
        bytecount::count(&local_buffer[..total_read], b'\n')
    }).sum();

    Ok(newline_count)
}

/// Count newlines in a reader (e.g., stdin) sequentially.
fn count_newlines_in_reader<R: Read>(mut reader: R) -> io::Result<usize> {
    let mut total_newlines = 0;
    let mut buffer = vec![0u8; CHUNK_SIZE];

    loop {
        let bytes_read = reader.read(&mut buffer[..])?;

        if bytes_read == 0 {
            break;
        }

        // Count newlines in the buffer
        let chunk_newlines = bytecount::count(&buffer[..bytes_read], b'\n');
        total_newlines += chunk_newlines;
    }

    Ok(total_newlines)
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    // Determine input source
    let input_source = if args.len() == 1 || args[1] == "-" {
        // Read from stdin
        None
    } else if args.len() == 2 {
        Some(args[1].clone())
    } else {
        eprintln!("Usage: {} [file_path]", args[0]);
        std::process::exit(1);
    };

    let newline_count = if let Some(file_path) = input_source {
        count_newlines_in_file(&file_path)?
    } else {
        // Read from stdin
        count_newlines_in_reader(io::stdin())?
    };

    println!("{}", newline_count);

    Ok(())
}
