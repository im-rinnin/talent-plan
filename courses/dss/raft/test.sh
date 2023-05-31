set -e 

cargo check

x=1
while [ $x -le 5 ]
do
cargo test my_test
cargo test 2a
cargo test 2b 
# cargo test 2c
 x=$(( $x + 1 ))
done
