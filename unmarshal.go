package ddb

// Unmarshal all will run through all results in 'r' and  scans the results into 'v'. It will
// consume 'r' in the process and it cannot be used after.
func UnmarshalAll(r Result, v interface{}) (err error) {

	for r.Next() {
		if err = r.Scan(nil); err != nil {
			return
		}
	}

	if err = r.Err(); err != nil {
		return
	}

	return
}
