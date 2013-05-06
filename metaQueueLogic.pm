#
#	metaQueueLogic.pm
#Thu May  2 15:01:57 CEST 2013

use MooseX::Declare;
use MooseX::NonMoose;
use MooseX::MarkAsMethods;

class My::Schema {
	has 'spool' => ( isa => 'Str', is => 'rw');
	has 'backendId' => ( isa => 'Str', is => 'rw', default => 'OGS');
	has 'backend' => ( isa => 'metaQueue', is => 'rw');

	# <p> dependencies
	use TempFileNames;
	use Set;
	use Data::Dumper;
	use POSIX qw(strftime mktime);
	
	after backendId($id) {
		my $metaQclass = 'metaQueue'. $id;
		$self->backend($metaQclass->new());
	};

	method queue($commands, $dependencies) {
		my $q = $self->resultset('Queue');
		my $deps = [map { { id_depends_on => $_ } } @$dependencies];
		for my $command (@$commands) {
			my $dict = {
				file_name => $command,
				submission_date => strftime('%Y-%m-%d %H:%M:%S', localtime),
				dependency_id_jobs => $deps
			};
			my $r = $q->new_result($dict);
			$r->insert;
		}
	};
}

class metaQueue {
	method queue($command) {
	}
}

class metaQueueOGS extends metaQueue {
	use TempFileNames;
	use Set;
	use Data::Dumper;

	
	method queue($command) {
		Log("Hello from OGS ");
	}

}
