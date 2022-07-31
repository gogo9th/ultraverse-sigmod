#!/usr/bin/env python

import random
import time
import datetime
import subprocess
import sys
import os
import django
from abc import ABC, abstractmethod

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'zabacus.settings')
os.environ.setdefault('DJANGO_SECRET_KEY', '!2%a%fw)m((glocz!3y*wh8j_zb#)t65-q(5mqz&mczld8-4eb')
django.setup()

from zabacus.bills.schema import BenchmarkUpdateBillName, BenchmarkUpdateBillItemName
from zabacus.bills.schema import CreateBill, AddBillItem, AddUserToBill
from django.db.state_db import STATE_DB_BLOCK_EXECUTE, STATE_DB_ABORT
from django.contrib.auth import get_user_model
from django.core import management
from datetime import timedelta
from zabacus.bills.models import Bill, BillItem, Involvement, ItemWeightAssignment, BillStatus


class BenchParams:
    # max_operations = 10000
    max_operations = 500
    # Number of initial users in the database
    # User names are assumed to be 'user00001' to 'user00100'
    # IDs are 1-100
    # num_init_users = 100
    num_init_users = 10
    # Number of initial bills in the database
    # IDs are 1-1000
    # num_init_bills = 1000
    num_init_bills = 50
    # Range of number of users per bill, [min, max]
    num_users_per_bill = (3, 6)
    # Range of number of bill items per bill, [min, max]
    # Each bill item involve all users in the bill
    num_items_per_bill = (5, 15)
    # Create a new user after executing this many operations
    # create_user_every_x_ops = 500
    create_user_every_x_ops = 500
    # Create a new bill after executing this many operations
    # create_bill_every_x_ops = 100
    create_bill_every_x_ops = 100
    # Update the name of a bill after this many operations
    update_bill_name_every_x_ops = 105
    # Update the first bill item name in a bill after this many operations
    update_bill_item_name_every_x_ops = 105


def user_info_by_id(user_id):
    build_obj = lambda **kwargs: type("Object", (), kwargs)
    return build_obj(context=build_obj(user=get_user_model().objects.get(id=str(user_id))))


def anonymous_user_info():
    build_obj = lambda **kwargs: type("Object", (), kwargs)
    return build_obj(context=build_obj(user=build_obj(is_anonymous=True)))


def username_from_id(user_id):
    return 'user{:05d}'.format(user_id)


class Job(ABC):
    @abstractmethod
    def run(self):
        assert False, 'Abstract method called.'


class CreateBillJob(Job):
    def __init__(self, creator_id, num_users, num_items, benchmark_runner):
        self.creator_id = creator_id
        self.num_users = num_users
        self.num_items = num_items
        self.benchmark_runner = benchmark_runner

    def run(self):
        new_bill_id = CreateBill().mutate(user_info_by_id(self.creator_id), 'Example Bill',
                                          desc='Blah blah blah').bill.id
        self.benchmark_runner.incr_bill_count()
        bill_users = set()
        bill_users.add(self.creator_id)
        for i in range(self.num_users - 1):
            while True:
                rand_user = random.randint(
                    1, self.benchmark_runner.current_user_count())
                if rand_user not in bill_users:
                    bill_users.add(rand_user)
                    break
        for rand_user in bill_users:
            if rand_user == self.creator_id:
                continue
            self.benchmark_runner.append_job(
                AddUserToBillJob(self.creator_id, new_bill_id, username_from_id(rand_user)))
        for i in range(self.num_items):
            self.benchmark_runner.append_job(
                AddBillItemJob(new_bill_id, bill_users))
        self.benchmark_runner.record_user_with_bill(self.creator_id)


class UpdateBillNameJob(Job):
    def __init__(self, creator_id, new_name):
        self.creator_id = creator_id
        self.new_name = new_name

    def run(self):
        BenchmarkUpdateBillName().mutate(user_info_by_id(self.creator_id), self.new_name)


class UpdateBillItemNameJob(Job):
    def __init__(self, creator_id, new_item_name):
        self.creator_id = creator_id
        self.new_item_name = new_item_name

    def run(self):
        BenchmarkUpdateBillItemName().mutate(user_info_by_id(self.creator_id), self.new_item_name)


class AddUserToBillJob(Job):
    def __init__(self, creator_id, bill_id, username):
        self.creator_id = creator_id
        self.bill_id = bill_id
        self.username = username

    def run(self):
        AddUserToBill().mutate(user_info_by_id(self.creator_id), self.bill_id, self.username)


class AddBillItemJob(Job):
    def __init__(self, bill_id, bill_users):
        self.bill_id = bill_id
        self.bill_users = bill_users

    def run(self):
        weights = {}
        for uid in self.bill_users:
            weights[username_from_id(uid)] = 10.00
        payer_id = random.sample(self.bill_users, 1)[0]
        AddBillItem().mutate(user_info_by_id(payer_id), self.bill_id, 'random_item_name', 'random_description',
                             payer_id, 10.00 * len(self.bill_users), weights)


class CreateUserJob(Job):
    def __init__(self, new_user_id, benchmark_runner):
        self.username = username_from_id(new_user_id)
        self.benchmark_runner = benchmark_runner

    def run(self):
        user = get_user_model()(
            username=self.username,
            first_name=self.username + 'First',
            last_name=self.username + 'Last',
            email='first.last@example.com'
        )
        user.set_password('1234567')
        user.save()

        self.benchmark_runner.incr_user_count()


class BenchmarkRunner:
    def __init__(self):
        self.op_count = 0
        self.user_count = 0
        self.user_id_gen = 0
        self.bill_count = 0
        self.max_ops = BenchParams.max_operations
        self.users_with_bill = set()
        self.job_queue = list()

        self.pre_populate_begin_time = None
        self.benchmark_begin_time = None
    def current_user_count(self):
        return self.user_count

    def incr_bill_count(self):
        self.bill_count += 1

    def incr_user_count(self):
        self.user_count += 1

    def append_job(self, job):
        self.job_queue.append(job)

    def pick_random_user(self):
        return random.randint(1, self.user_count)

    def record_user_with_bill(self, user_id):
        self.users_with_bill.add(user_id)

    def pick_random_user_with_bill(self):
        return random.sample(self.users_with_bill, 1)[0]

    def pick_random_bill(self):
        return random.randint(1, self.bill_count)

    @staticmethod
    def pick_num_users():
        return random.randint(*BenchParams.num_users_per_bill)

    @staticmethod
    def pick_num_items():
        return random.randint(*BenchParams.num_items_per_bill)

    def add_create_bill_job(self):
        self.append_job(CreateBillJob(self.pick_random_user(),
                                      self.pick_num_users(), self.pick_num_items(), self))

    def pre_populate(self):
        self.pre_populate_begin_time = time.time()

        # Prepopulate users and bills
        print('>>> Insert initial users.')
        for i in range(BenchParams.num_init_users):
            CreateUserJob(i + 1, self).run()
        assert self.user_count == BenchParams.num_init_users
        self.user_id_gen = self.user_count

        print('>>> Insert initial bills.')
        for i in range(BenchParams.num_init_bills):
            CreateBillJob(self.pick_random_user(), self.pick_num_users(
            ), self.pick_num_items(), self).run()
        assert self.bill_count == BenchParams.num_init_bills

        # Drain the job queue
        print('>>> Insert initial user-bill relations and bill items.')
        while self.job_queue:
            if len(self.job_queue) % 100 == 0:
                print(
                    '>>>>> {} pre-population ops remaining.'.format(len(self.job_queue)))
            self.job_queue.pop(0).run()

    def benchmark(self, scenario):
        self.benchmark_begin_time = time.time()

        while self.op_count <= self.max_ops:
            self.op_count += 1
            if not self.job_queue:
                self.add_create_bill_job()
            # Pop job off run queue and run it
            self.job_queue.pop(0).run()

            if self.op_count % BenchParams.create_bill_every_x_ops == 0:
                print('>>> Queue CreateBill job at position={}.'.format(
                    len(self.job_queue)))
                self.add_create_bill_job()
            if self.op_count % BenchParams.create_user_every_x_ops == 0:
                self.user_id_gen += 1
                print('>>> Queue CreateUser job at position={}.'.format(
                    len(self.job_queue)))
                self.append_job(CreateUserJob(self.user_id_gen, self))

            if scenario == 1:
                if self.op_count % BenchParams.update_bill_name_every_x_ops == 0:
                    self.append_job(UpdateBillNameJob(self.pick_random_user_with_bill(), 'new random bill name'))
            elif scenario == 2:
                if self.op_count % BenchParams.update_bill_item_name_every_x_ops == 0:
                    self.append_job(UpdateBillItemNameJob(self.pick_random_user_with_bill(), 'new random bill item name'))

            if self.op_count % 50 == 0:
                print('>>> Completed {} operations.'.format(self.op_count))

    def summary(self):
        elapsed0 = self.benchmark_begin_time - self.pre_populate_begin_time
        elapsed1 = time.time() - self.benchmark_begin_time

        print('Pre-population time: %.6f sec.' % elapsed0)
        print('Elapsed time: %.6f sec.' % elapsed1)
        print('Throughput: %.2f operations/sec.' % (BenchParams.max_operations / elapsed1))


BASE_SCRIPT_PATH = None


def run_query(query):
    command = "source %s; echo '%s' | mysql -u$MYSQL_ID -p$MYSQL_PW" % (BASE_SCRIPT_PATH, query)
    return subprocess.check_output(['/bin/bash', '-c', command])


def run_base_script(is_modify_mode, args):
    command = 'source ' + BASE_SCRIPT_PATH
    
    # if is_modify_mode is True:
    #     command += '; set_test_modified'
    # else:
    #     command += '; set_test_original'
    command += '; set_test_modified'
    command += '; DB_MODE="M"'

    command += '; ' + args[0]

    for i in args[1:]:
        command += ' "%s"' % i

    return subprocess.run(['/bin/bash', '-c', command])


def dump_database(filepath):
    out = subprocess.check_output(['/bin/bash', '-c', 'mysqldump -uroot -p123456 --databases zabacus'])
    with open(filepath, 'wb') as f:
        f.write(out)
        f.close()


def create_database():
    subprocess.check_output(['/bin/bash', '-c', "echo 'DROP DATABASE IF EXISTS `zabacus`' | mysql -uroot -p123456"])
    subprocess.check_output(['/bin/bash', '-c', "echo 'CREATE DATABASE `zabacus`' | mysql -uroot -p123456"])


def init(is_modify_mode):
    if run_base_script(is_modify_mode, ['kill_mysqld']).returncode > 0:
        return False
    if run_base_script(is_modify_mode, ['delete_log']).returncode > 0:
        return False
    if run_base_script(is_modify_mode, ['copy_config']).returncode > 0:
        return False
    if run_base_script(is_modify_mode, ['run_mysqld']).returncode > 0:
        return False

    create_database()

    from django.db.backends.mysql.base import DatabaseWrapper
    from django.db import connection, Error

    if connection.display_name is not DatabaseWrapper.display_name:
        return False

    cursor = connection.cursor()

    try:
        cursor.execute("USE `zabacus`")
    except Error as e:
        print(e)
        return False

    return True


def make_user_query_file(query):
    filepath = 'user_query.sql'

    with open(filepath, 'w') as f:
        for i in query:
            f.write(i + ';\n')
        f.close()

    return filepath


def run_my_query_1(benchmark_runner):
    obj = ItemWeightAssignment.objects.first()

    origin_amount = obj.amount

    obj.amount = 9999
    obj.save()

    obj.amount = origin_amount
    obj.save()


def run_my_query_2(new_name, benchmark_runner):
    benchmark_runner.job_queue.clear()

    benchmark_runner.append_job(UpdateBillNameJob(benchmark_runner.pick_random_user_with_bill(), new_name))

    while len(benchmark_runner.job_queue) > 0:
        benchmark_runner.job_queue.pop(0).run()

def run_my_query_3(new_name, benchmark_runner):
    benchmark_runner.job_queue.clear()

    benchmark_runner.append_job(UpdateBillItemNameJob(benchmark_runner.pick_random_user_with_bill(), new_name))

    while len(benchmark_runner.job_queue) > 0:
        benchmark_runner.job_queue.pop(0).run()

def flashback_run(scenario_no):
    # flash back - Run
    if init(True) is False:
        print('flash_back init Error.')
        exit()

    from django.db import transaction
    transaction.set_autocommit(False)

    management.call_command('migrate')
    transaction.commit()

    flash_back = BenchmarkRunner()

    print('Pre-populating database...')
    flash_back.pre_populate()
    transaction.commit()
    print('Pre-populating database...done')

    dump_database('flash_back_pre_populate_done.sql')

    undo_time = time.time()

    if scenario_no == 0:
        run_my_query_1(flash_back)
    elif scenario_no == 1:
        run_my_query_2('*****', flash_back)
    else:
        run_my_query_3('*****', flash_back)
    transaction.commit()

    redo_time = time.time()

    dump_database('flash_back_add_custom_done.sql')

    print('Start benchmarking...')
    flash_back.benchmark(scenario_no)
    transaction.commit()
    print('Start benchmarking...done')

    dump_database('flash_back_bench_mark_done.sql')

    print('Start Flash-back...')
    flash_back_begin = time.time()

    run_query('SET GLOBAL FOREIGN_KEY_CHECKS=0')

    ret = run_base_script(
        True,
        ['flashback_doit',
         datetime.datetime.fromtimestamp(undo_time).strftime('%Y-%m-%d %H:%M:%S.%f'),
         datetime.datetime.fromtimestamp(redo_time).strftime('%Y-%m-%d %H:%M:%S.%f'),
         "zabacus"
        ]).returncode

    run_query('SET GLOBAL FOREIGN_KEY_CHECKS=1')

    if ret != 0:
        print('flash_back run Error.')
        exit()

    flash_back_end = time.time()
    print('Start Flash-back...end')

    dump_database('flash_back_done.sql')

    print('---------------------------------------------------------------------------')
    print('Result---------------------------------------------------------------------')
    print('---------------------------------------------------------------------------')
    flash_back.summary()
    print('Flash-back time: %.6f sec.' % (flash_back_end - flash_back_begin))
    print('End.')


def db_changer_run(scenario_no):
    # db_state_change - Run
    if init(True) is False:
        print('db_changer init Error.')
        exit()

    from django.db import transaction
    management.call_command('migrate')
    transaction.commit()

    db_changer = BenchmarkRunner()

    print('Pre-populating database...')
    db_changer.pre_populate()
    transaction.commit()
    print('Pre-populating database...done')

    dump_database('db_changer_pre_populate_done.sql')

    if scenario_no == 0:
        state_id = STATE_DB_BLOCK_EXECUTE(run_my_query_1, db_changer)
    elif scenario_no == 1:
        state_id = STATE_DB_BLOCK_EXECUTE(run_my_query_2, '*****', db_changer)
    else:
        state_id = STATE_DB_BLOCK_EXECUTE(run_my_query_3, '*****', db_changer)
    transaction.commit()

    if state_id is None:
        print('STATE_DB_BLOCK_EXECUTE() Error.')
        exit()

    dump_database('db_changer_add_custom_done.sql')

    print('Start benchmarking...')
    db_changer.benchmark(scenario_no)
    transaction.commit()
    print('Start benchmarking...done')

    dump_database('db_changer_bench_mark_done.sql')

    print('Start STATE_DB_ABORT...')
    db_changer_begin = time.time()
    STATE_DB_ABORT(state_id)
    db_changer_end = time.time()
    transaction.commit()
    print('Start STATE_DB_ABORT...done')

    dump_database('db_changer_done.sql')

    print('---------------------------------------------------------------------------')
    print('Result---------------------------------------------------------------------')
    print('---------------------------------------------------------------------------')
    db_changer.summary()
    print('db_state_change time: %.6f sec.' % (db_changer_end - db_changer_begin))
    print('End.')


def print_help(name):
    print ("%s [-o or -m] [0 or 1 or 2]" % name)
    print ("    -o    original mariadb test")
    print ("    -m    modified mariadb test")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print_help(sys.argv[0])
        exit()

    if sys.argv[1] == '-o':
        is_modify_mode = False
    elif sys.argv[1] == '-m':
        is_modify_mode = True
    else:
        print_help(sys.argv[0])
        exit()

    scenario_no = int(sys.argv[2])

    BASE_SCRIPT_PATH = '../base_script/mysql_env.sh'

    path = subprocess.check_output(['/bin/bash', '-c', 'source ' + BASE_SCRIPT_PATH + '; set_test_modified; echo $DB_STATE_CHANGE_PATH'])
    django.db.state_db.STATE_GROUP_BIN_PATH = path.decode().strip()

    if len(django.db.state_db.STATE_GROUP_BIN_PATH) < 1:
        print('Failed to get STATE_GROUP_BIN_PATH')
        exit()

    print('About to start benchmarking.')
    print('!!!WARNING!!! This operation will destroy the existing database.')
    confirmation = input('Continue? (y/n[n])')
    if confirmation not in ['y', 'Y']:
        print('Aborted.')
        exit()

    print('Starting to execute benchmark, scenario {}.'.format(scenario_no))

    if is_modify_mode is True:
        db_changer_run(scenario_no)
    else:
        flashback_run(scenario_no)
