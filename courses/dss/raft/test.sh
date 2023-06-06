set -e 

cargo check

x=1
while [ $x -le 30 ]
do
# RUST_BACKTRACE=1 cargo test est_figure_8_unreliable

# RUST_LOG=info RUST_BACKTRACE=1 cargo test est_snapshot_basic_2d  >res 2>&1
# cargo test my_test
cargo test 2 
# cargo test test_snapshot_basic_2d
# cargo test 2b 
# cargo test 2c
 x=$(( $x + 1 ))
done
