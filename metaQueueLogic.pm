#
#	metaQueueLogic.pm
#Thu May  2 15:01:57 CEST 2013

use MooseX::Declare;
use MooseX::NonMoose;
use MooseX::MarkAsMethods;

class My::Schema {
	has 'spool' => ( isa => 'Str', is => 'r');

}

class meta_queueQGS {
	use TempFileNames;
	use Set;
	use Data::Dumper;
	use POSIX qw(strftime mktime);
	use POSIX::strptime qw(strptime);
	use utf8;

	
	method greetings() {
		Log("Hello world ". $self->spool);
	}

}
