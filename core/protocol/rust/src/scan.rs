//! Validate raw JSON before deserializing, including fields the codec ignores.

use std::collections::BTreeSet;

use crate::{Error, MAX_DEPTH, MAX_WIRE_BYTES};

pub(crate) fn check(input: &[u8]) -> Result<(), Error> {
    if input.len() > MAX_WIRE_BYTES {
        return Err(Error::MessageTooLarge);
    }
    std::str::from_utf8(input).map_err(|_| Error::InvalidJson)?;
    let mut scan = Scanner { input, offset: 0 };
    scan.value(0)?;
    scan.whitespace();
    if scan.offset != input.len() {
        return Err(Error::InvalidJson);
    }
    Ok(())
}

struct Scanner<'a> {
    input: &'a [u8],
    offset: usize,
}

impl Scanner<'_> {
    fn peek(&self) -> Option<u8> {
        self.input.get(self.offset).copied()
    }

    fn whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\r' | b'\n')) {
            self.offset += 1;
        }
    }

    fn take(&mut self, expected: u8) -> Result<(), Error> {
        if self.peek() != Some(expected) {
            return Err(Error::InvalidJson);
        }
        self.offset += 1;
        Ok(())
    }

    fn value(&mut self, depth: usize) -> Result<(), Error> {
        self.whitespace();
        match self.peek() {
            Some(b'{') => self.object(depth + 1),
            Some(b'[') => self.array(depth + 1),
            Some(b'"') => self.string().map(|_| ()),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => Err(Error::InvalidJson),
        }
    }

    fn object(&mut self, depth: usize) -> Result<(), Error> {
        if depth > MAX_DEPTH {
            return Err(Error::NestingExceeded);
        }
        self.take(b'{')?;
        self.whitespace();
        if self.peek() == Some(b'}') {
            self.offset += 1;
            return Ok(());
        }
        let mut keys = BTreeSet::new();
        loop {
            let key = self.string()?;
            if !keys.insert(key) {
                return Err(Error::InvalidJson);
            }
            self.whitespace();
            self.take(b':')?;
            self.value(depth)?;
            self.whitespace();
            match self.peek() {
                Some(b'}') => {
                    self.offset += 1;
                    return Ok(());
                }
                Some(b',') => {
                    self.offset += 1;
                    self.whitespace();
                }
                _ => return Err(Error::InvalidJson),
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<(), Error> {
        if depth > MAX_DEPTH {
            return Err(Error::NestingExceeded);
        }
        self.take(b'[')?;
        self.whitespace();
        if self.peek() == Some(b']') {
            self.offset += 1;
            return Ok(());
        }
        loop {
            self.value(depth)?;
            self.whitespace();
            match self.peek() {
                Some(b']') => {
                    self.offset += 1;
                    return Ok(());
                }
                Some(b',') => self.offset += 1,
                _ => return Err(Error::InvalidJson),
            }
        }
    }

    fn string(&mut self) -> Result<String, Error> {
        let start = self.offset;
        self.take(b'"')?;
        loop {
            match self.peek() {
                Some(b'"') => {
                    self.offset += 1;
                    // String decoding rejects invalid escapes, lone UTF-16 surrogates,
                    // and control characters, and normalizes keys before duplicate checks.
                    return serde_json::from_slice(&self.input[start..self.offset])
                        .map_err(|_| Error::InvalidJson);
                }
                Some(b'\\') => {
                    self.offset += 1;
                    if self.peek().is_none() {
                        return Err(Error::InvalidJson);
                    }
                    self.offset += 1;
                }
                Some(0..=0x1f) | None => return Err(Error::InvalidJson),
                Some(_) => self.offset += 1,
            }
        }
    }

    fn literal(&mut self, expected: &[u8]) -> Result<(), Error> {
        if self.input.get(self.offset..self.offset + expected.len()) != Some(expected) {
            return Err(Error::InvalidJson);
        }
        self.offset += expected.len();
        Ok(())
    }

    fn digits(&mut self) -> Result<(), Error> {
        let start = self.offset;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.offset += 1;
        }
        if self.offset == start {
            return Err(Error::InvalidJson);
        }
        Ok(())
    }

    fn number(&mut self) -> Result<(), Error> {
        let start = self.offset;
        if self.peek() == Some(b'-') {
            self.offset += 1;
        }
        match self.peek() {
            Some(b'0') => self.offset += 1,
            Some(b'1'..=b'9') => self.digits()?,
            _ => return Err(Error::InvalidJson),
        }
        if self.peek() == Some(b'.') {
            self.offset += 1;
            self.digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.offset += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.offset += 1;
            }
            self.digits()?;
        }
        if self.offset - start > 128 {
            return Err(Error::InvalidJson);
        }
        // Parent delimiters and the final EOF check reject a numeric prefix
        // followed by garbage. No float/integer conversion touches opaque numbers.
        Ok(())
    }
}
