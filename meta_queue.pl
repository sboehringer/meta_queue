#!/usr/bin/perl
#	meta_queue.pl
#Thu May  2 13:39:43 CEST 2013

use TempFileNames;
use Set;
use Data::Dumper;
use DBI;
use DBIx::Class::Schema::Loader qw(make_schema_at);
use Module::Load;
use LWP::Simple;
use POSIX qw(strftime mktime);
use POSIX::strptime qw(strptime);
use utf8;


# default options
$main::d = {
	# triggers
	queue => \&queue,
	createdb => \&create_db,
	qstat => \&qstat,

	# defaults
	config => 'meta_queue.cfg',
	location => "$ENV{HOME}/.local/share/applications/meta_queue",
};
# options
$main::o = [
];

# default options
$main::d = { config => 'config.cfg', 'hello' => sub { print "hello world\n"; } };
# options
$main::o = ['simple', 'filter|f=s', 'int=i', 'onOff!', 'credentials'];
$main::usage = '';
$main::helpText = <<HELP_TEXT.$TempFileNames::GeneralHelp;
	there is no specific help.

HELP_TEXT

my $sqlitedb = <<DBSCHEMA;
	CREATE TABLE queue (
		id integer primary key autoincrement,
		file_name text not null
	);
DBSCHEMA

sub instantiate_db { my ($c) = @_;
	my $dbfile = "$c->{location}/meta_queue.db";
	return if (-e $dbfile);
	System("mkdir --parents $c->{location} ; echo '$sqlitedb\n.quit' | sqlite3 $dbfile", 2);
}

sub dump_schema { my ($c) = @_;
	my $dbfile = "$c->{location}/meta_queue.db";
	# DBIx schema
	my $schemadir = "$c->{location}/schema";
	Log("Dump dir: $schemadir");
	make_schema_at('My::Schema',
		{ debug => 1, dump_directory => $schemadir },
		[ "dbi:SQLite:dbname=$dbfile", '', '']
	);
}

sub create_db { my ($c) = @_;
	instantiate_db($c);
	dump_schema($c);
}

#main $#ARGV @ARGV %ENV
	#initLog(2);
	my $c = StartStandardScript($main::d, $main::o);
exit(0);
