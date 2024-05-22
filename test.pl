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
    while (my $row = $csv->getline($fh)) {
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
                      VALUES (\"$sensor_id\", \"$sensor_name\", \"$current_time\")") 
                or
            warn "ERROR: Cannot insert into table: row number $rn, $sensor_name, $sensor_id";
        }
        my $sensor_id = $sensor_hash{$sensor_name};
        my $current_time = DateTime->now->datetime();
        if ($dt_correct && $mt_correct) {
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            my $data_id = datetime_pure_str($dt).random_string(10);
            my $time = $row->[0]; 
            $dbh->do("INSERT INTO T_DATA (
                        DATA_ID, 
                        TAKEN_AT, 
                        SENSOR_ID, 
                        SENSOR_READING,  
                        SENSOR_NAME, 
                        CREATED_AT, 
                        VALIDITY
                      ) VALUES (
                        \"$data_id\", 
                        \"$time\", 
                        \"$sensor_id\", 
                        $meter,  
                        \"$sensor_name\", 
                        \"$current_time\", 
                        \"Y\"
                      )") 
                or
            warn "ERROR: Cannot insert into table: row number $rn --- $dt, $sensor_name, $meter";
        }
        elsif ($dt_correct && !$mt_correct) {
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            print "Data corrupted on row number $rn, $file: Sensor reading invalid\n";
            my $data_id = datetime_pure_str($dt).random_string(10);
            my $time = $row->[0]; 
            $dbh->do("INSERT INTO T_DATA (
                        DATA_ID, 
                        TAKEN_AT, 
                        SENSOR_ID, 
                        ORIGINAL_SENSOR_READING, 
                        SENSOR_NAME, 
                        CREATED_AT, 
                        VALIDITY
                      ) VALUES (
                        \"$data_id\", 
                        \"$time\", 
                        \"$sensor_id\", 
                        \"$meter\",  
                        \"$sensor_name\", 
                        \"$current_time\", 
                        \"N\"
                      )") 
                or
            warn "ERROR: Cannot insert into table: row number $rn --- $dt, $sensor_name, $meter";
        }
        elsif (!$dt_correct && $mt_correct) {
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            print "Data corrupted on row number $rn, $file: Datetime invalid\n";
            my $data_id = "NULL".random_string(10);
            $dbh->do("INSERT INTO T_DATA (
                        DATA_ID, 
                        TAKEN_AT, 
                        SENSOR_ID, 
                        SENSOR_READING, 
                        SENSOR_NAME, 
                        CREATED_AT, 
                        VALIDITY
                      ) VALUES (
                        \"$data_id\", 
                        NULL, 
                        \"$sensor_id\",
                        $meter,  
                        \"$sensor_name\", 
                        \"$current_time\", 
                        \"N\"
                      )") 
                or
            warn "ERROR: Cannot insert into table: row number $rn --- $dt, $sensor_name, $meter";
        }
        else{       # !$dt_correct && !$mt_correct
#           print $row->[0], " ", $row->[1], " ", $row->[2], "\n";
            print "Data corrupted on row number $rn, $file: DATA NOT PROCESSED\n"; 
        }
    }
    close $fh;
}






# ===================================================
# =               SUPPORTIVE FUNCTIONS              = 
# ===================================================

sub datetime_pure_str {
    my $dt = $_[0];      # datetime object
    return $dt->ymd("")."T".$dt->hms("");
}
