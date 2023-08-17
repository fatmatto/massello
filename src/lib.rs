#![deny(clippy::all)]
/// This works derives from https://github.com/kylebarron/parquet-wasm

#[macro_use]
extern crate napi_derive;

use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use napi::bindgen_prelude::Buffer;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{fs::File, sync::Arc};


/// Reads a parquet buffer. You can pass columns to specify which columns should be read
/// It returns data into arrow IPC format.
#[napi]
pub fn from_parquet_buffer(parquet_buffer: Buffer, columns: Vec<String>) -> Vec<u8> {
    // Create Parquet reader
    let buf: Vec<u8> = parquet_buffer.into();
    let cursor: Bytes = buf.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor).unwrap();

    let mut arrow_schema = builder.schema().clone();

    if columns.len() > 0 {
        let column_positions: Vec<usize> = columns.iter().map(|column_name: &String| arrow_schema.index_of(column_name).unwrap()).collect();
        arrow_schema = Arc::new(arrow_schema.project(&column_positions).unwrap());
    }

    // Create Arrow reader
    let reader = builder.build().unwrap();

    // Create IPC Writer
    let mut output_file = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema).unwrap();
        // Iterate over record batches, writing them to IPC stream
        for maybe_record_batch in reader {
            let record_batch = maybe_record_batch.unwrap();
            writer.write(&record_batch).unwrap();
        }
        writer.finish().unwrap();
    }

    output_file
}


#[napi]
pub fn from_parquet_file(path: String, columns: Vec<String>) -> Vec<u8> {
    let file = File::open(path).unwrap();
    // let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    // let schema = file_reader.metadata().file_metadata().schema();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();


    let mut arrow_schema = builder.schema().clone();

    if columns.len() > 0 {
        let column_positions: Vec<usize> = columns.iter().map(|column_name: &String| arrow_schema.index_of(column_name).unwrap()).collect();
        arrow_schema = Arc::new(arrow_schema.project(&column_positions).unwrap());
    }

    let  reader = builder.build().unwrap();
    let mut output_file = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema).unwrap();

        // Iterate over record batches, writing them to IPC stream
        for maybe_record_batch in reader {
            let record_batch = maybe_record_batch.unwrap();
            writer.write(&record_batch).unwrap();
        }
        writer.finish().unwrap();
    }

    // Note that this returns output_file directly instead of using writer.into_inner().to_vec() as
    // the latter seems likely to incur an extra copy of the vec
    output_file

}