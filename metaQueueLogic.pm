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
	has 'backendQueueLimit' => ( isa => 'Int', is => 'rw' );

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
				job_script => $job,
				job_options => $options,
				submission_date => strftime('%Y-%m-%d %H:%M:%S', localtime),
				dependency_id_jobs => $deps,
			};
			my $r = $q->new_result($dict);
			$r->insert;
			Log(sprintf("Spooled '%s' to job_id %d", splitPathDict($command)->{base}, $r->id), 1);
		}
	};

	# forward jobs to backend
	method queuePush() {
		#
		# <p> find finished jobs
		#
		my @stillRunning = $self->backend->queued();
		my $q = $self->resultset('Queue')->search_rs({
			backend_completion_date => undef,
			backend_submission_date => { '!=' => undef },
			#id_backend => { -in => [@stillRunning]}
		});
		# <p> update job status to finished
		my $job;
		for (; $job = $q->next; ) {
			next if (defined(indexOf(\@stillRunning, $job->id_backend)));
			Log('job  '. $job->id_backend. ' -> finished', 5);
			my $ji = $self->backend->jobInfo($job->id_backend);
			$job->exit_code($ji->{exit_code});
			$job->completion_date(strftime('%Y-%m-%d %H:%M:%S', localtime));
			$job->backend_completion_date($ji->{completion_time});
			$job->update;

		}
		#$q = $q->update({ completion_date => strftime('%Y-%m-%d %H:%M:%S', localtime)});

		#
		# <p> submit new jobs
		#

		# set page size
		Log("#jobs: ". int(@stillRunning). "; limit:". $self->backendQueueLimit, 5);
		my $h = $self->resultset('Queue',
			rows => $self->backendQueueLimit - int(@stillRunning)
		)->search_rs({ backend_submission_date => undef });
		for my $job ($h->page(0)->all) {
			my $idBackend = $self->backend->queue($job);
			$job->id_backend($idBackend);
			$job->backend_submission_date(strftime('%Y-%m-%d %H:%M:%S', localtime));
			$job->update;
			Log('Job submitted. Backend id: '. $idBackend, 5);
		}
	}
}

class metaQueue {
	use TempFileNames;
	my $queueTempDir = tempFileName("/tmp/perl_tmp_$ENV{USER}/meta_queue");

	has 'config' => ( isa => 'Hash', is => 'ro');
	has 'tempDir' => ( isa => 'Str', is => 'rw', default => $queueTempDir, required => 1 );

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
		Log("Temp dir: ". $self->tempDir, 5);
		my $tf = tempFileName($self->tempDir. '/job', undef, { doTouch => 1 });
		writeFile($tf, $job->job_script);

		Log("qsub script:\n-- Start of script --\n". $job->job_script. "\n-- End of script --\n", 5);
		my $r = System("qsub $tf", 4, undef, { returnStdout => 'YES' } );
		#Stdout:
		#Your job 710 ("job_echo34686.sh") has been submitted
		my ($jid) = ($r->{output} =~ m{Your job (\d+)}so);
		return $jid;
	}

	method queued() {
		my @jobs = (`qstat -u \\* -xml | xml sel -t -m '//JB_job_number' -v 'text()' -o ' '`
			=~ m{(\d+)}sog);
	}

	#perl -e 'print join(":", (`qacct -j 13073` =~ m{^([a-z_]+)\s+(.*?)\s*$}moig))'
	method jobInfo($id_backend) {
		my %attr = (`qacct -j 13073` =~ m{^([a-z_]+)\s+(.*?)\s*$}moig);
		print(Dumper(\%attr));
		#<!> time format
		return { exit_code => $attr{exit_status}, completion_time => $attr{end_time} };
	}

}
