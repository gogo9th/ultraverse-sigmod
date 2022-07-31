python3 ./run_test.py hash ../tatp_run.sh -m1
mv tatp_run_158* tatp_run_hash_hashon_update_3

python3 ./run_test.py hash ../tatp_run.sh -m2
mv tatp_run_158* tatp_run_hash_hashoff_update_3

python3 ./run_test.py hash ../tatp_run.sh -o
mv tatp_run_158* tatp_run_hash_flashback_update_3

python3 ./run_test.py normal ../tatp_run.sh -m1
mv tatp_run_158* tatp_run_normal_hashon_update_3

python3 ./run_test.py normal ../tatp_run.sh -m2
mv tatp_run_158* tatp_run_normal_hashoff_update_3

python3 ./run_test.py normal ../tatp_run.sh -o
mv tatp_run_158* tatp_run_normal_flashback_update_3

