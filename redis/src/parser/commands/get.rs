struct Get {
    key: String,
}

macro_rules! cmd {
    ($c:expr) => {
        fn $c(i: &[u8]) -> IResult<&[u8], &[u8]> {
        let (i, str) = (string)(i)?;
        let (j, cmd) = tag($c)(str)?;
        assert!(j.is_empty());
        Ok((i, cmd))
    }
    };
}

cmd!("GET");