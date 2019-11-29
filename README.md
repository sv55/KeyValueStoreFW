# KeyValueStoreFW

## File Structure
Key Value store has been implemented using two files. The 'keys' and 'values' file. 'values' file will contain the value length and the actual value. 'keys' file will have the key name, the offset of 'values' file from where current key's value is present and the expiry time of the key.
The key metadata is loaded into the memory since it's of very small size. For every get, offset is read from the key metadata and a disk read is performed to obtain the value. This makes the implementation very memory efficient.

## Compaction
Since the keys and values are appended to existing data, data must be compacted periodically to delete unwanted data. This is done by a background thread. It reads all the existing key's data into a new file and replaces the old one. This compaction is done once in every two minutes if any deletes have been done or any of the keys have expired.

### PS
All the requirements given in the document have been implemented.
- Test.java contains the testcases.
- Only one program could access the store at a time.
- The store is thread safe, multiple threads could access.
- All the necessary size checks have been done.
