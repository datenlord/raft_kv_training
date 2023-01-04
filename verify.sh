#!/bin/sh

output_help_message() {
cat <<- EOF 
Usage: $0 [OPTION] [TARGET]
OPTIONS:
    single  - run the given test, like "$0 single test_nonleader_start_election"
    file    - run all the tests in the given file, like "$0 file log"
    batch   - run the given tests in batch, like "$0 batch 2aa" or "$0 batch 2aa_initial"

TARGETS:
    - 2aa: indicates the initial and election tests
        - 2aa_initial:      cluster initial state tests
        - 2aa_election:     leader election tests
        - 2aa_heartbeat:    heartbeat tests
    - 2ab: indicates the log replication tests
        - 2ab_append:       log replication tests

EOF
}

run_single_test() {
    cargo test $1
}

run_tests_in_a_file() {
    cargo test consensus::$1::tests 
}

run_batch_tests() {
    cargo test $1 
}

case $1 in
help)
    output_help_message
    ;;
single)
    run_single_test $2
    ;;
file)
    run_tests_in_a_file $2
    ;;
batch)
    run_batch_tests $2
    ;;
*)
cat <<- EOF
    Usage: $0 [OPTION] [TARGET]
    For more details, running "$0 help"
EOF
    exit 1
esac