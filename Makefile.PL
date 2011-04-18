use strict;
use inc::Module::Install;

name 'Queue-Q4M-Worker';
all_from 'lib/Queue/Q4m/Worker.pm';

requires 'DBI';
requires 'POSIX';
requires 'Parallel::Prefork';

auto_set_repository;

WriteAll;