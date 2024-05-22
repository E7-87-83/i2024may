use v5.30.0;
use warnings;
use Crypt::PRNG qw/random_string rand/;
use DateTime;

for (1..1) {
    my $filename = random_string(6);
    open FH, ">", "$filename.csv";
    my @sensor_names = ("MPF Sensor", "APA Reader", "XYZ Reader", "Thermometer", "Humidity Sensor");

    my $start_ep = DateTime->new(year => 2020, month => 5, day => 30, hour => 8, minute => 30, second => 0)->epoch;
    my $end_ep = DateTime->now->epoch;

    sub rand_date {
        my $dt = DateTime->from_epoch(
            epoch => $start_ep + rand()*($end_ep-$start_ep)
        );
        return ($dt->datetime())."+08:00";
    }

    say FH "timestamp,sensorName,value";
    for (1..100) {
        say FH rand_date().",".$sensor_names[$#sensor_names*rand()].",".rand(100);
    }

}
