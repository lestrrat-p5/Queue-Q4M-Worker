package Queue::Q4M::Worker;
use strict;
use DBI;
use POSIX qw(:signal_h);
use Class::Accessor::Lite
    rw => [ qw(
        dbh
        max_workers
        signal_received
        sql
        _work_once
    ) ]
;

our $VERSION = '0.02';

my $guard;
BEGIN {
    if ( eval { require Scope::Guard } && !$@ ) {
        $guard = \&Scope::Guard::guard;
    } else {
        *Queue::Q4M::Worker::Guard::DESTROY = sub {
            if (! $_[0][0]) {
                $_[0]->();
            }
        };
        $guard = sub { bless [ 1, $_[0] ], 'Queue::Q4M::Worker::Guard' };
    }
}

sub new {
    my ($class, %args) = @_;

    bless {
        max_workers => 0,
        _work_once => delete $args{work_once},
        %args
    }, $class;
}

sub _get_sql {
    my $self = shift;
    my $sql = $self->sql;

    my ($stmt, @binds);
    if (ref $sql eq 'CODE') {
        ($stmt, @binds) = $sql->($self);
    } else {
        $stmt = $sql;
    }
    return ($stmt, @binds);
}

sub _get_dbh {
    my $self = shift;
    my $dbh = $self->dbh;

    my $handle;
    if ( ref $dbh eq 'CODE' ) {
        $handle = $dbh->($self);
    } else {
        $handle = $dbh;
    }
    return $handle;
}

sub work_once {
    my $self = shift;
    if ( my $cb = $self->_work_once) {
        return $cb->( $self, @_ );
    }
}

sub should_loop { ! $_[0]->signal_received }
sub work {
    my $self = shift;

    if ( $self->max_workers > 1 ) {
        $self->run_multi();
    } else {
        $self->run_single();
    }
}

# Run multiple children using Parallel::Prefork (if you want more
# control over how this is done, please subclass).
sub run_multi {
    my $self = shift;
    my $pp = Paralle::Prefork->new({
        max_workers => $self->max_workers,
        trap_signals => {
            TERM => 'TERM',
            HUP  => 'TERM',
            INT  => 'TERM',
        }
    });

    while ( $pp->signal_received ne 'TERM' ) {
        $pp->start(sub { $self->run_single });
    }

    $pp->wait_all_chlidren()
}

sub run_single {
    my $self = shift;
    my $dbh = $self->_get_dbh();
    my $sth;
    my $sigset = POSIX::SigSet->new( SIGINT, SIGQUIT, SIGTERM );
    my $cancel_q4m = POSIX::SigAction->new(sub {
        my $signame = shift;
        eval { $sth->cancel };
        eval { $dbh->disconnect };
        $self->signal_received( $signame );
    }, $sigset, &POSIX::SA_NOCLDSTOP);
    my $install_sig = sub {
        # XXX use SigSet to properly interrupt the process
        POSIX::sigaction( SIGINT,  $cancel_q4m );
        POSIX::sigaction( SIGQUIT, $cancel_q4m );
        POSIX::sigaction( SIGTERM, $cancel_q4m );
    };

    $install_sig->();

    my $default_sig = POSIX::SigAction->new('DEFAULT');
    while ( $self->should_loop ) {
        my ($stmt, @binds) = $self->_get_sql();
        $sth = $dbh->prepare( $stmt );
        my $rv = $sth->execute( @binds );
        if ( $rv == 0 ) { # nothing
            $sth->finish;
            next;
        }

        while (
            ! $self->signal_received &&
            $self->should_loop &&
            ( my $h = $sth->fetchrow_hashref )
        ) {
            # while the consumer is working, we need to reset the
            # signal handlers that we previously set
            my $gobj = $guard->($install_sig);
            POSIX::sigaction( SIGINT,  $default_sig );
            POSIX::sigaction( SIGQUIT, $default_sig );
            POSIX::sigaction( SIGTERM, $default_sig );

            $self->work_once( $h );
        }
    }
    POSIX::sigaction( SIGINT,  $default_sig );
    POSIX::sigaction( SIGQUIT, $default_sig );
    POSIX::sigaction( SIGTERM, $default_sig );
}

1;

__END__

=head1 NAME

Queue::Q4M::Worker - Worker Object Receiving Items From Q4M

=head1 SYNOPSIS

    use Queue::Q4M::Worker;

    my $worker = Queue::Q4M::Worker->new(
        sql => "SELECT * FROM my_queue WHERE queue_wait(...)",
        max_workers => 10, # use Parallel::Prefork
        work_once => sub {
            my ($worker, $row) = @_;
            # $row is a HASH
        }
    );

    $worker->work;

=head1 DESCRIPTION

Queue::Q4M::Worker abstracts a worker subscribing to a Q4M queue.

=head1 CAVEATS

This is a proof of concept release. Please report bugs, and send pull
requests if you like the idea.

=head1 AUTHOR

Daisuke Maki C<< <daisuke@endeworks.jp> >>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Daisuke Maki

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.0 or,
at your option, any later version of Perl 5 you may have available.

=cut


