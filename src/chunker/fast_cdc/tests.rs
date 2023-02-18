use super::*;
use md5::{Digest, Md5};
use std::fs::{self, File};

#[test]
fn test_logarithm2() {
    assert_eq!(logarithm2(0), 0);
    assert_eq!(logarithm2(1), 0);
    assert_eq!(logarithm2(2), 1);
    assert_eq!(logarithm2(3), 2);
    assert_eq!(logarithm2(5), 2);
    assert_eq!(logarithm2(6), 3);
    assert_eq!(logarithm2(11), 3);
    assert_eq!(logarithm2(12), 4);
    assert_eq!(logarithm2(19), 4);
    assert_eq!(logarithm2(64), 6);
    assert_eq!(logarithm2(128), 7);
    assert_eq!(logarithm2(256), 8);
    assert_eq!(logarithm2(512), 9);
    assert_eq!(logarithm2(1024), 10);
    assert_eq!(logarithm2(16383), 14);
    assert_eq!(logarithm2(16384), 14);
    assert_eq!(logarithm2(16385), 14);
    assert_eq!(logarithm2(32767), 15);
    assert_eq!(logarithm2(32768), 15);
    assert_eq!(logarithm2(32769), 15);
    assert_eq!(logarithm2(65535), 16);
    assert_eq!(logarithm2(65536), 16);
    assert_eq!(logarithm2(65537), 16);
    assert_eq!(logarithm2(1_048_575), 20);
    assert_eq!(logarithm2(1_048_576), 20);
    assert_eq!(logarithm2(1_048_577), 20);
    assert_eq!(logarithm2(4_194_303), 22);
    assert_eq!(logarithm2(4_194_304), 22);
    assert_eq!(logarithm2(4_194_305), 22);
    assert_eq!(logarithm2(16_777_215), 24);
    assert_eq!(logarithm2(16_777_216), 24);
    assert_eq!(logarithm2(16_777_217), 24);
}

#[test]
#[should_panic]
fn test_minimum_too_low() {
    let array = [0u8; 1024];
    StreamCDC::new(array.as_slice(), 63, 256, 1024);
}

#[test]
#[should_panic]
fn test_minimum_too_high() {
    let array = [0u8; 1024];
    StreamCDC::new(array.as_slice(), 67_108_867, 256, 1024);
}

#[test]
#[should_panic]
fn test_average_too_low() {
    let array = [0u8; 1024];
    StreamCDC::new(array.as_slice(), 64, 255, 1024);
}

#[test]
#[should_panic]
fn test_average_too_high() {
    let array = [0u8; 1024];
    StreamCDC::new(array.as_slice(), 64, 268_435_457, 1024);
}

#[test]
#[should_panic]
fn test_maximum_too_low() {
    let array = [0u8; 1024];
    StreamCDC::new(array.as_slice(), 64, 256, 1023);
}

#[test]
#[should_panic]
fn test_maximum_too_high() {
    let array = [0u8; 1024];
    StreamCDC::new(array.as_slice(), 64, 256, 1_073_741_825);
}

#[test]
fn test_masks() {
    let source = [0u8; 1024];
    let chunker = StreamCDC::new(source.as_slice(), 64, 256, 1024);
    assert_eq!(chunker.mask_l, MASKS[7]);
    assert_eq!(chunker.mask_s, MASKS[9]);
    let chunker = StreamCDC::new(source.as_slice(), 8192, 16384, 32768);
    assert_eq!(chunker.mask_l, MASKS[13]);
    assert_eq!(chunker.mask_s, MASKS[15]);
    let chunker = StreamCDC::new(source.as_slice(), 1_048_576, 4_194_304, 16_777_216);
    assert_eq!(chunker.mask_l, MASKS[21]);
    assert_eq!(chunker.mask_s, MASKS[23]);
}

#[test]
fn test_cut_all_zeros() {
    // for all zeros, always returns chunks of maximum size
    let array = [0u8; 10240];
    let mut chunker = StreamCDC::new(array.as_slice(), 64, 256, 1024);
    let mut cursor: usize = 0;
    for _ in 0..10 {
        let ChunkData { hash, offset, length, .. } = chunker.read_chunk().unwrap();
        let pos = offset as usize + length;
        assert_eq!(hash, 14169102344523991076);
        assert_eq!(pos, cursor + 1024);
        cursor = pos;
    }
    // assert that nothing more should be returned
    assert!(matches!(chunker.read_chunk(), Err(Error::Empty)));
}

#[test]
fn test_cut_sekien_16k_chunks() {
    let contents = fs::read("test/fixtures/SekienAkashita.jpg").unwrap();
    let mut chunker = StreamCDC::new(contents.as_slice(), 4096, 16384, 65535);
    let mut cursor: usize = 0;
    let mut remaining: usize = contents.len();
    let expected = [
        (17968276318003433923, 21325),
        (8197189939299398838, 17140),
        (13019990849178155730, 28084),
        (4509236223063678303, 18217),
        (2504464741100432583, 24700),
    ];
    for (e_hash, e_length) in expected.iter() {
        let ChunkData { hash, offset, length, .. } = chunker.read_chunk().unwrap();
        let pos = offset as usize + length;
        assert_eq!(hash, *e_hash);
        assert_eq!(pos, cursor + e_length);
        cursor = pos;
        remaining -= e_length;
    }
    assert_eq!(remaining, 0);
}

#[test]
fn test_cut_sekien_32k_chunks() {
    let contents = fs::read("test/fixtures/SekienAkashita.jpg").unwrap();
    let mut chunker = StreamCDC::new(contents.as_slice(), 8192, 32768, 131072);
    let mut cursor: usize = 0;
    let mut remaining: usize = contents.len();
    let expected =
        [(15733367461443853673, 66549), (6321136627705800457, 42917)];
    for (e_hash, e_length) in expected.iter() {
        let ChunkData { hash, offset, length, .. } = chunker.read_chunk().unwrap();
        let pos = offset as usize + length;
        assert_eq!(hash, *e_hash);
        assert_eq!(pos, cursor + e_length);
        cursor = pos;
        remaining -= e_length;
    }
    assert_eq!(remaining, 0);
}

#[test]
fn test_cut_sekien_64k_chunks() {
    let contents = fs::read("test/fixtures/SekienAkashita.jpg").unwrap();
    let mut chunker = StreamCDC::new(contents.as_slice(), 16384, 65536, 262144);
    let mut cursor: usize = 0;
    let mut remaining: usize = contents.len();
    let expected = [(2504464741100432583, 109466)];
    for (e_hash, e_length) in expected.iter() {
        let ChunkData { hash, offset, length, .. } = chunker.read_chunk().unwrap();
        let pos = offset as usize + length;
        assert_eq!(hash, *e_hash);
        assert_eq!(pos, cursor + e_length);
        cursor = pos;
        remaining -= e_length;
    }
    assert_eq!(remaining, 0);
}

struct ExpectedChunk {
    hash: u64,
    offset: u64,
    length: usize,
    digest: String,
}

#[test]
fn test_iter_sekien_16k_chunks() {
    let contents = fs::read("test/fixtures/SekienAkashita.jpg").unwrap();
    // The digest values are not needed here, but they serve to validate
    // that the streaming version tested below is returning the correct
    // chunk data on each iteration.
    let expected_chunks = [
        ExpectedChunk {
            hash: 17968276318003433923,
            offset: 0,
            length: 21325,
            digest: "2bb52734718194617c957f5e07ee6054".into(),
        },
        ExpectedChunk {
            hash: 8197189939299398838,
            offset: 21325,
            length: 17140,
            digest: "badfb0757fe081c20336902e7131f768".into(),
        },
        ExpectedChunk {
            hash: 13019990849178155730,
            offset: 38465,
            length: 28084,
            digest: "18412d7414de6eb42f638351711f729d".into(),
        },
        ExpectedChunk {
            hash: 4509236223063678303,
            offset: 66549,
            length: 18217,
            digest: "04fe1405fc5f960363bfcd834c056407".into(),
        },
        ExpectedChunk {
            hash: 2504464741100432583,
            offset: 84766,
            length: 24700,
            digest: "1aa7ad95f274d6ba34a983946ebc5af3".into(),
        },
    ];
    let mut chunker = StreamCDC::new(contents.as_slice(), 4096, 16384, 65535);
    let mut index = 0;
    while let Some(Ok(chunk)) = chunker.next() {
        assert_eq!(chunk.hash, expected_chunks[index].hash);
        assert_eq!(chunk.offset, expected_chunks[index].offset);
        assert_eq!(chunk.length, expected_chunks[index].length);

        let offset = chunk.offset as usize;
        let mut hasher = Md5::new();
        hasher.update(&contents[offset..offset + chunk.length]);
        let table = hasher.finalize();
        let digest = format!("{table:x}");
        assert_eq!(digest, expected_chunks[index].digest);
        index += 1;
    }
    assert_eq!(index, 5);
}

#[test]
fn test_cut_sekien_16k_nc_0() {
    let contents = fs::read("test/fixtures/SekienAkashita.jpg").unwrap();
    let mut chunker = StreamCDC::with_level(contents.as_slice(), 4096, 16384, 65535, Normalization::Level0);
    let mut cursor: usize = 0;
    let mut remaining: usize = contents.len();
    let expected = [
        (443122261039895162, 6634),
        (15733367461443853673, 59915),
        (10460176299449652894, 25597),
        (6197802202431009942, 5237),
        (6321136627705800457, 12083),
    ];
    for &(e_hash, e_length) in expected.iter() {
        let ChunkData { hash, offset, length, .. } = chunker.read_chunk().unwrap();
        let pos = offset as usize + length;
        assert_eq!(hash, e_hash);
        assert_eq!(pos, cursor + e_length);
        cursor = pos;
        remaining -= e_length;
    }
    assert_eq!(remaining, 0);
}

#[test]
fn test_cut_sekien_16k_nc_3() {
    let contents = fs::read("test/fixtures/SekienAkashita.jpg").unwrap();
    let mut chunker = StreamCDC::with_level(contents.as_slice(), 8192, 16384, 32768, Normalization::Level3);
    let mut cursor: usize = 0;
    let mut remaining: usize = contents.len();
    let expected = [
        (10718006254707412376, 17350),
        (13104072099671895560, 19911),
        (12322483109039221194, 17426),
        (16009206469796846404, 17519),
        (2473608525189754172, 19940),
        (2504464741100432583, 17320),
    ];
    for (e_hash, e_length) in expected.iter() {
        let ChunkData { hash, offset, length, .. } = chunker.read_chunk().unwrap();
        let pos = offset as usize + length;
        assert_eq!(hash, *e_hash);
        assert_eq!(pos, cursor + e_length);
        cursor = pos;
        remaining -= e_length;
    }
    assert_eq!(remaining, 0);
}

#[test]
fn test_error_fmt() {
    let err = Error::Empty;
    assert_eq!(format!("{err}"), "chunker error: Empty");
}

#[test]
fn test_stream_sekien_16k_chunks() {
    let file = File::open("test/fixtures/SekienAkashita.jpg").unwrap();
    // The set of expected results should match the non-streaming version.
    let expected_chunks = [
        ExpectedChunk {
            hash: 17968276318003433923,
            offset: 0,
            length: 21325,
            digest: "2bb52734718194617c957f5e07ee6054".into(),
        },
        ExpectedChunk {
            hash: 8197189939299398838,
            offset: 21325,
            length: 17140,
            digest: "badfb0757fe081c20336902e7131f768".into(),
        },
        ExpectedChunk {
            hash: 13019990849178155730,
            offset: 38465,
            length: 28084,
            digest: "18412d7414de6eb42f638351711f729d".into(),
        },
        ExpectedChunk {
            hash: 4509236223063678303,
            offset: 66549,
            length: 18217,
            digest: "04fe1405fc5f960363bfcd834c056407".into(),
        },
        ExpectedChunk {
            hash: 2504464741100432583,
            offset: 84766,
            length: 24700,
            digest: "1aa7ad95f274d6ba34a983946ebc5af3".into(),
        },
    ];
    let chunker = StreamCDC::new(Box::new(file), 4096, 16384, 65535);
    let mut index = 0;
    for result in chunker {
        let chunk = result.unwrap();
        assert_eq!(chunk.hash, expected_chunks[index].hash);
        assert_eq!(chunk.offset, expected_chunks[index].offset);
        assert_eq!(chunk.length, expected_chunks[index].length);
        let mut hasher = Md5::new();
        hasher.update(&chunk.data);
        let table = hasher.finalize();
        let digest = format!("{table:x}");
        assert_eq!(digest, expected_chunks[index].digest);
        index += 1;
    }
    assert_eq!(index, 5);
}