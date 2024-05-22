use strict;
use warnings;
use threads;
use Thread::Queue;
use DBI;
use Config::Any;
use Crypt::PRNG qw/random_string/;
use DateTime::Format::ISO8601;
use DateTime;
use Text::CSV_XS qw/csv/;
use File::Spec;
use Scalar::Util qw/looks_like_number/;

my $dir_to_be = $ARGV[0];

die "No input directory path\n" if !defined($ARGV[0]);

my @config_files = "config.yaml";
my $cfg = Config::Any->load_files( {
    files=> \@config_files, 
    use_ext => 0, 
    flatten_to_hash => 1 
});

my ($database, $hostname, $port, $dbuser, $dbuserpw) = (
    $cfg->{$config_files[0]}->{database},
    $cfg->{$config_files[0]}->{hostname},
    $cfg->{$config_files[0]}->{port},
    $cfg->{$config_files[0]}->{dbuser},
    $cfg->{$config_files[0]}->{dbuserpw},        
);



my $queue = Thread::Queue->new;

opendir(my $dh, $dir_to_be) || die "Can't open $dir_to_be: $!";
my @files;

while (readdir $dh) {
    push @files, $_ if $_ =~ /\.csv$/ || $_ =~ /\.txt$/;
}

foreach my $file (@files) {
    $queue->enqueue($file);
}

my @threads;
for (1..4) {
    my $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";
    push @threads, async {
       my $dbh = DBI->connect($dsn, $dbuser, $dbuserpw);
       while (my $file = $queue->dequeue_nb) {
           # Process the file in parallel
           process_file($dir_to_be, $file, $dbh);
       }
       $dbh->disconnect();
   };
}

$_->join() for @threads;
print "PROCESSING DONE\n";





# ===================================================
# =                 PROCESS_FILE                    = 
# ===================================================

sub process_file {
    my ($dir, $file, $dbh) = @_;
    # Perform the file processing logic here
    print "Processing file: $file\n";

    my @rows;
    # Read/parse CSV
    my $csv = Text::CSV_XS->new ({ binary => 1, auto_diag => 1 });
    open my $fh, "<:encoding(utf8)", File::Spec->catfile($dir, $file) 
        or die "$file: $!";
    my $rn = 0;
    my %sensor_hash;
    my @records;
              
    while (my $row = $csv->getline($fh)) {
        next if !defined($row->[0]);
        $rn++;
        my ($dt_correct, $mt_correct) = (1,1);
        my $dt = "null";
        eval {$dt = DateTime::Format::ISO8601->parse_datetime($row->[0]);}
                 or 
        $dt_correct = 0;
        my $meter = $row->[2];
        $mt_correct = 0 if !looks_like_number($meter) || $meter =~ /inf/;
        my $sensor_name = $row->[1];
        if (!defined($sensor_hash{$sensor_name})) {
            $sensor_hash{$sensor_name} = $dt . random_string(16);
            my $sensor_id = $sensor_hash{$sensor_name};
            my $current_time = DateTime->now->datetime();
            $dbh->do("INSERT INTO T_SENSOR (SENSOR_ID, SENSOR_NAME, CREATED_AT) 
                      VALUES (\"$sensor_id\", \"$sensor_name\", \"$current_time\")"); 
        }
        my $sensor_id = $sensor_hash{$sensor_name};
        my $current_time = DateTime->now->datetime();
        if ($dt_correct && $mt_correct) {
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            my $data_id = datetime_pure_str($dt).random_string(10);
            my $time = $row->[0]; 
            push @records, [$data_id, $time, $sensor_id, $meter, undef, $sensor_name, $current_time, "Y"]
        }
        elsif ($dt_correct && !$mt_correct) {
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            print "Data corrupted on row number $rn, $file: Sensor reading invalid\n";
            my $data_id = datetime_pure_str($dt).random_string(10);
            my $time = $row->[0]; 
            push @records, [$data_id, $time, $sensor_id, undef, $meter, $sensor_name, $current_time, "N"]
        }
        elsif (!$dt_correct && $mt_correct) {
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            print "Data corrupted on row number $rn, $file: Datetime invalid\n";
            my $data_id = "NULL".random_string(10);
            push @records, [$data_id, undef, $sensor_id, $meter, undef, $sensor_name, $current_time, "N"]
        }
        else{       # !$dt_correct && !$mt_correct
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            print "Data corrupted on row number $rn, $file: DATA NOT PROCESSED\n"; 
        }
        print "$rn\n" if $rn % 10_000 == 0; # testing
        if (@records % 100_000 == 0) {
            insert_into_t_data($dbh , [@records]);
            print "insert record up to row $rn\n";
            @records = ();
        }
    }
    if (@records != 0) {
        insert_into_t_data($dbh , [@records]);
        print "insert record up to row $rn, $file DONE";
        @records = ();
    }
    close $fh;
}

sub insert_into_t_data {
    my $dbh = $_[0];
    my @records = $_[1]->@*;
    my $values = join ", ", ("( ?, ?, ?, ?,   ?, ?, ?, ?)") x @records;
    my $sth = $dbh->prepare("INSERT INTO T_DATA (
                DATA_ID, 
                TAKEN_AT, 
                SENSOR_ID, 
                SENSOR_READING,
                ORIGINAL_SENSOR_READING,
                SENSOR_NAME, 
                CREATED_AT, 
                VALIDITY
              ) VALUES $values");
    $sth->execute(map { @$_ } @records);
}

# ===================================================
# =               SUPPORTIVE FUNCTIONS              = 
# ===================================================

sub datetime_pure_str {
    my $dt = $_[0];      # datetime object
    return $dt->ymd("")."T".$dt->hms("");
}
