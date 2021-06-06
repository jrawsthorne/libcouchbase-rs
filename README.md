Play around with the Couchbase protocol in Rust

Currently, every second, this inserts a document, gets that document, asserts it is equal, deletes it, gets again and asserts it's None

# Steps

- Install Couchbase
- Load the travel-sample bucket
- Change host and credentials
- cargo run
- try adding a node to the cluster while running
