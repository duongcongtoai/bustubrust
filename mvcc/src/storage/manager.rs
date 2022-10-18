use std::{
    fs::OpenOptions,
    os::unix::{io::IntoRawFd, prelude::OpenOptionsExt},
    ptr::{null, null_mut},
};

use libc::c_void;

pub struct StorageManager {
    filepath: String,
}

impl StorageManager {
    pub fn allocate(&self, size: usize) -> *mut c_void {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o777)
            .open(&self.filepath)
            .expect("open mmap file");
        let fd = file.into_raw_fd();
        let data_file_len = 512 * 1024 * 1024;

        unsafe {
            let ret = libc::posix_fallocate(fd, 0, data_file_len);
            if ret != 0 {
                panic!("poxi_fallocate: {}", ret);
            }
            let data_file_addr = libc::mmap(
                null_mut(),
                data_file_len as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if data_file_addr == libc::MAP_FAILED {
                panic!("mmap failed");
            }
            return data_file_addr;
        };
    }
}
