#
#	metaQueueLogic.pm
#Thu May  2 15:01:57 CEST 2013

use MooseX::Declare;
use MooseX::NonMoose;
use MooseX::MarkAsMethods;

class My::Schema {
	has 'backendConfigs' => ( isa => 'Any', is => 'rw');
	has 'spool' => ( isa => 'Str', is => 'rw', writer => 'set_spool');
	has 'backendId' => ( isa => 'Str', is => 'rw', default => 'OGS');
	has 'backend' => ( isa => 'metaQueue', is => 'rw');
	has 'backendQueueLimit' => ( isa => 'Int', is => 'rw', default => 1e3 );

	# <p> dependencies
	use TempFileNames;
	use Set;
	use Data::Dumper;
	use POSIX qw(strftime mktime);
	use File::Copy;

	after backendId($id) {
		my $metaQclass = 'metaQueue'. $id;
		$self->backend($metaQclass->new($self->backendConfigs->{$id}));
	};

	after set_spool($path) {
		Log(sprintf("Creating spool folder '%s'\n", $path), 3);
		Mkdir($path);
	};
	
	method queue($commands, $options, $dependencies) {
		my $q = $self->resultset('Queue');
		my $deps = [map { { id_depends_on => $_ } } @$dependencies];
		for my $command (@$commands) {
			my $job = readFile($command);
			if (!defined($job)) {
				Log("Spooling '$command' failed (could not be read).", 1);
				next;
			}
			my $dict = {
				job_path => $command,
				job_file => $job,
				job_options => $options,
				submission_date => strftime('%Y-%m-%d %H:%M:%S', localtime),
				dependency_id_jobs => $deps,
			};
			my $r = $q->new_result($dict);
			$r->insert;
			my $spoolFile = sprintf('%s/%07d_%s', $self->spool, $r->id, splitPathDict($command)->{base});
			my $oneOnSuccess = copy($command, $spoolFile);
			if (!$oneOnSuccess) {
				Log("Spooling '$command' to '$spoolFile' failed. Rolling back.", 1);
				my $rsDel = $self->resultset('Queue')->search({ id => $r->id });
				$rsDel->delete;
			} else {
				Log(sprintf("Spooled '$command' with id %d",  $r->id), 1);
			}
		}
	};

	# forward jobs to backend
	method queuePush() {
		# <p> find finished jobs
		my @jobs = $self->backend->queued();
		my $q = $self->resultset('Queue');
		my @finished = 	$q->search_rs({
			completion_date => { '==' => undef },
			id_backend => { -in => [@jobs]}
		});
		# <p> update job status to finished
		$q = $q->update({ completion_date => strftime('%Y-%m-%d %H:%M:%S', localtime)});

		# <p> submit new jobs
		my $h = $self->resultset('Queue');
		# set page size
		$h->rows($self->backendQueueLimit - int(@jobs));
		$h->search_rs({ completion_date => { '==' => undef } });
		$self->backend->queue($_) for ($h->page(0));
	}
}

class metaQueue {
	has 'config' => ( isa => 'Hash', is => 'ro');

	method queue($job) {
		die 'abstract class';
	}
	method queued() {
		die 'abstract class: to return ids of scheduled jobs';
	}
}

class metaQueueOGS extends metaQueue {
	use TempFileNames;
	use Set;
	use Data::Dumper;

	
	method queue($job) {
		Log("Hello from OGS ");
	}

	method queued() {
		my @jobs = (`qstat -u \\* -xml | xml sel -t -m '//JB_job_number' -v 'text()' -o ' '`
			=~ m{(\d+)}sog);
	}
}
