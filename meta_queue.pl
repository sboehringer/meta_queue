#!/usr/bin/perl
#	meta_queue.pl
#Thu May  2 13:39:43 CEST 2013

use TempFileNames;
use Set;
use Data::Dumper;
use DBI;
use DBIx::Class::Schema::Loader qw(make_schema_at);
use Module::Load;
use POSIX qw(strftime mktime);
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
	spool => "$ENV{HOME}/.local/share/applications/meta_queue/spool",
	backend => 'OGS',
};
# options
$main::o = [
	'dependsOn=s'
];

$main::usage = '';
$main::helpText = <<HELP_TEXT.$TempFileNames::GeneralHelp;
	Options:
	--dependsOn id1,id2,...	Hold property for jobs: wait for these jobs\
				to finish before scheduling

HELP_TEXT

my $sqlitedb = <<DBSCHEMA;
	CREATE TABLE queue (
		id integer primary key autoincrement,
		id_backend integer,
		job_path text not null,
		job_script text not null,	-- content of job_path
		job_options text,
		submission_date date not null,
		completion_date date,
		exit_code integer
	);
	CREATE INDEX queue_idx ON queue (id);
	CREATE INDEX queue_idx2 ON queue (id_backend);
	CREATE INDEX queue_idx3 ON queue (completion_date);

	CREATE TABLE dependency (
		id_job integer not null references queue(id),
		id_depends_on integer not null references queue(id),
		UNIQUE(id_job, id_depends_on)
	);
	CREATE INDEX dependency_idx ON dependency (id_job, id_depends_on);
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

#
#	<i> determine setter method from class
#
sub meta_setter { my ($obj, $dict, $keys) = @_;
	$keys = makeHash($keys, $keys) if (ref($keys) eq 'ARRAY');
	for my $key (keys %$keys) {
		$obj->$key($dict->{$keys->{$key}});
	}
	return $obj;
}

sub load_db { my ($c) = @_;
	my $dbfile = "$c->{location}/meta_queue.db";
	my $schemadir = "$c->{location}/schema";
	unshift(@INC, ($schemadir, '.'));
	load('My::Schema');
	load('metaQueueLogic');
	my $schema = My::Schema->connect("dbi:SQLite:dbname=$dbfile", '', '');
	#my $schema = My::Schema->new(spool => $c->{spool}, backendId => $c->{backend});
	#$schema->connect("dbi:SQLite:dbname=$dbfile", '', '');
	$schema->backendConfigs($c->{backends});
	$schema->backendId($c->{backend});
	meta_setter($schema, $c, {'backends' => 'backendConfigs', 'backend' => 'backendId'});
	return $schema;
}

sub queue { my ($c, @commands) = @_;
	load_db($c)->queue([@commands], firstDef($c->{options}, ''), [split(/\s*,\s*/, $c->{dependsOn})]);
}

#main $#ARGV @ARGV %ENV
	#initLog(2);
	my $c = StartStandardScript($main::d, $main::o);
exit(0);
