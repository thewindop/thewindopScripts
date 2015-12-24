#!/usr/bin/perl 
#-d:NYTProf
# For profiling

=head1
  dataFilter.pl : Used to bin exported windop csv data.

  Copyright (C) 2015  thewindop.com

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

  Contact: andy\@thewindop.com
  
=cut

###############################################################################
# PERL modules to load
###############################################################################
use strict;
use DateTime;
use Getopt::Long;
use Data::Dumper;        # Data dumper
use FindBin qw($Bin);    # get path

###############################################################################
# initialise script, process input
###############################################################################
my $start           = time();
my $fetchEnd        = $start;
my $inputDataPoints = 0;
my %resultsHash;
my %configHash;

initialiseConfigHash( \%configHash );

GetOptions(
  "minutes=s"   => \$configHash{minutes},
  "startdate=s" => \$configHash{startdate},
  "enddate=s"   => \$configHash{enddate},
  "device=s"    => \$configHash{device},
  "inFile=s"    => \$configHash{inFile},
  "outFile=s"   => \$configHash{outFile},
  "verbose=s"   => \$configHash{verbose},
  "dumper"      => \$configHash{dumper},
  "help"        => \$configHash{help}
);

###############################################################################
# global variable/constants for speedup
###############################################################################
our $dt_jj;
our %constHash;
$constHash{angles} = [ "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW", "N" ];

# Set a global variable that defines the logging level.
our $myGlobal__LoggingLevel = $configHash{verbose};

###############################################################################
# lets run!
###############################################################################
if ( $configHash{help} ) {
  printHelpText();
} else {
  printLicenseHeader();
  my $count      = 0;
  my $totalcount = 0;
  my $outStr     = getUniqueTimeStampSecDelay();
  my @localDataArray;

  initResultsSearchHash( \%resultsHash );

  $resultsHash{state}      = "searching";
  $resultsHash{binendtime} = getUniqueTimeStampSecDelay();

  # check inout file exists
  if ( -e $configHash{inFile} ) {

    # open the CSV for this days data and slurp the contents
    my $allDataRef = readFileContentsRetArrayRef( "", $configHash{inFile} );

    # take the headings of the slurped array, remove the new line. We assume line one
    # always has heading for each column. Always true for data exported by thewindop, unless modified.
    my $headings = shift(@$allDataRef);
    chomp $headings;    # remove newline
    processingHeadings( $headings, \%resultsHash );

    # loop over the data array, after the headings all subsequent lines represent an individual data sample
    foreach my $line ( sort { $a cmp $b } @$allDataRef ) {
      chomp($line);
      @localDataArray = split m/,/, $line;

      # We can filter the data on a specific date window
      if ( ( $configHash{startdate} < $localDataArray[0] ) & ( $configHash{enddate} > $localDataArray[0] ) ) {
        processValidRawDataPoint( "", \%resultsHash, \@localDataArray, $count, $configHash{minutes} );
        $count++;
      }
      $totalcount++;
    }

    # we are finished, but dont forget to finalise the final data if required. This is in
    # case we are still binning data, we want to show these values
    finaliseBinData( \%resultsHash, $resultsHash{binendtime} );

  } else {
    dateTimeLogging::windLogging::logOutInfo( 1, "$configHash{inFile} does not exist, removing from display" );
  }
  dateTimeLogging::windLogging::logOutInfo( 1, "Generating output file with $resultsHash{bincount} bins. Total count = $totalcount." );

  if ( $configHash{outFile} ne "" ) {
    writeToFile( "", $configHash{outFile}, buildDataFilterOutputCsvString( \%resultsHash ) );
  }
  $fetchEnd = time();
}

my $procEnd = time();
$fetchEnd -= $start;
dateTimeLogging::windLogging::logOutInfo( 1, "Execution time  = $fetchEnd seconds" );

#print Dumper \%resultsHash;

###############################################################################
# Process a CSV line in
# This function keeps track of the binning of the data points.
###############################################################################
sub processValidRawDataPoint {
  my ( $headHash, $outRef, $localDataArray, $count, $minuteBin ) = @_;

  $localDataArray->[0] =~ m/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/;

  if ( $outRef->{hourSynced} == -1 ) {
    dateTimeLogging::windLogging::logOutInfo( 1, "Enter search loop at $count\n" );
    $outRef->{lastHour} = $4;
    $outRef->{hourSynced}++;
  } else {
    if ( $outRef->{hourSynced} == 0 ) {
      if ( ( $5 % $minuteBin ) == 0 ) {
        dateTimeLogging::windLogging::logOutInfo( 1, "$localDataArray->[0] valid boundary at $5 after $count data points" );
        if ( $outRef->{state} eq "searching" ) {
          $outRef->{state} = "locked";
          my $temp = getNextBinEndTime( $localDataArray->[0], $minuteBin, 1 );
          if ( exists $outRef->{data}{ $outRef->{binendtime} } ) {
            $outRef->{data}{$temp} = delete $outRef->{data}{ $outRef->{binendtime} };
          }
          $outRef->{binendtime} = $temp;
          $outRef->{bincount}++;
          dateTimeLogging::windLogging::logOutInfo( 1, "Got a bin end time of $outRef->{binendtime}" );
        }
      }
      if ( $outRef->{lastHour} != $4 ) {
        dateTimeLogging::windLogging::logOutInfo( 1, "Hour synced after $count data points on hour $4\n" );
        $outRef->{hourSynced} = 1;
      }
    }
  }

  if ( $outRef->{binendtime} < $localDataArray->[0] ) {
    finaliseBinData( $outRef, $outRef->{binendtime} );
    $outRef->{binendtime} = getNextBinEndTime( $outRef->{binendtime}, $minuteBin, 0 );
    $outRef->{bincount}++;
    dateTimeLogging::windLogging::logOutInfo( 3, "Got a bin end time of $outRef->{binendtime} Bin No. $outRef->{bincount}" );
    $outRef->{data}{ $outRef->{binendtime} }{noInBin} = 1;
  } else {
    $outRef->{data}{ $outRef->{binendtime} }{noInBin}++;
  }

  for ( my $iter = 0 ; $iter < @$localDataArray ; $iter++ ) {
    if ( $outRef->{hmap}{$iter} ne "" ) {

      # protect against headings with no entry
      # check the kery is also valid for output, we put other data into the CSV heading we can ignore
      if ( exists $outRef->{validKeys}{ $outRef->{hmap}{$iter} } ) {
        updateValueForType( $outRef, $outRef->{binendtime}, $outRef->{hmap}{$iter}, @$localDataArray[$iter] );
      }
    }
  }
}

###############################################################################
# Generate the CSV output
# Note the points per bin count is added in here. This is the number of points
###############################################################################
sub buildDataFilterOutputCsvString {
  my ($outRef) = @_;
  my $outStr   = "";
  my $binCount = 0;
  foreach my $key ( sort { $a <=> $b } keys $outRef->{hmap} ) {

    # print the headings
    $outStr .= "$outRef->{hmap}{$key},";
    $binCount++;
  }

  # add the noInBin count to the headings to be output
  $outRef->{hmap}{$binCount} = "noInBin";
  $outStr .= "noInBin,\n";

  # loop over the data structure and dump all the data based on the headings
  # order that we just printed out
  foreach my $dataPoint ( sort { $a <=> $b } keys $outRef->{data} ) {
    foreach my $key ( sort { $a <=> $b } keys $outRef->{hmap} ) {
      if ( $outRef->{hmap}{$key} eq "time" ) {
        $outStr .= "$dataPoint,";
      } else {
        $outStr .= "$outRef->{data}{$dataPoint}{$outRef->{hmap}{$key}},";
      }
    }
    $outStr .= "\n";
  }

  #  print $outStr;
  return $outStr;
}

###############################################################################
# Find the corresponding direction for the reading taken.
###############################################################################
use constant DIRWEDGE  => 22.5;
use constant MINMAXOFF => DIRWEDGE / 2;
use constant WEDGEMASK => 0xF;

sub processWindDirReading {
  my ($reading) = @_;
  $reading += MINMAXOFF;    # add half the wedge width so we can div by wedge width
                            # Div by wedge, round down using int. And with 0xf to deal with wrapping.
  my $angle = ( int( $reading / DIRWEDGE ) ) & WEDGEMASK;
  return ( $angle, $constHash{angles}[$angle] );
}

###############################################################################
# Functions that operate on a full bin.
# Currently there are only 2, avg and domwd types.
# Average need to be divided by the number in that bin.
# DomanantWD need to iterate over all bins and choose the max
###############################################################################
sub finaliseBinData {
  my ( $outRef, $binEndTime ) = @_;

  foreach my $key ( keys $outRef->{data}{$binEndTime} ) {
    if ( $outRef->{validKeys}{$key} eq "avg" ) {

      # Average key types simply get divided by the number in that bin
      $outRef->{data}{$binEndTime}{$key} = sprintf( "%.2f", $outRef->{data}{$binEndTime}{$key} / $outRef->{data}{$binEndTime}{noInBin} );
    } elsif ( $outRef->{validKeys}{$key} eq "domwd" ) {

      # The dominant data hash may look like this, several bins will be full. We are looking
      # for the one with the highest count
      # {domwd}{w}{0}  = 20
      # {domwd}{w}{1}  = 40
      # {domwd}{w}{16} = 2
      #
      # Several new hash keys are used to test all the values
      # {domwd}{m} = m  The number of data points with that direction
      # {domwd}{d} = d  The current winning domanant direction
      #
      foreach my $wdir ( keys $outRef->{data}{$binEndTime}{$key}{w} ) {
        if ( !exists $outRef->{data}{$binEndTime}{$key}{m} ) {

          # First time round the loop. Create the data keys
          $outRef->{data}{$binEndTime}{$key}{m} = $outRef->{data}{$binEndTime}{$key}{w}{$wdir};
          $outRef->{data}{$binEndTime}{$key}{d} = $wdir;
          dateTimeLogging::windLogging::logOutInfo( 3, "New Dir bin $outRef->{data}{$binEndTime}{$key}{m} = wdir" );
        } else {
          if ( $outRef->{data}{$binEndTime}{$key}{m} < $outRef->{data}{$binEndTime}{$key}{w}{$wdir} ) {

            # Stored value is less than our current data point we have to update
            $outRef->{data}{$binEndTime}{$key}{m} = $outRef->{data}{$binEndTime}{$key}{w}{$wdir};
            $outRef->{data}{$binEndTime}{$key}{d} = $wdir;
            dateTimeLogging::windLogging::logOutInfo( 3, "Updated bin $outRef->{data}{$binEndTime}{$key}{m} = wdir" );
          }
        }
      }

      # Note here we are erasing all the working above and just storing the final result
      # If you are developing this method, you need to comment this next line out temporarily
      # This is to prevent the hash getting too large for big data sets
      $outRef->{data}{$binEndTime}{$key} = $outRef->{data}{$binEndTime}{$key}{d};
    }
  }
  $binEndTime =~ m/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/;

  # Write variant time formats to speed plotting in excel
  $outRef->{data}{$binEndTime}{eDateTime} = "$1/$2/$3 $4:$5:$6";
  $outRef->{data}{$binEndTime}{eDate}     = "$1/$2/$3";
  $outRef->{data}{$binEndTime}{eTime}     = "$4:$5:$6";

}

###############################################################################
# Function that selects correct operation to perform on an arriving data point
###############################################################################
sub updateValueForType {
  my ( $outRef, $binEndTime, $type, $value ) = @_;
  my $newKey = exists $outRef->{data}{$binEndTime}{$type} ? 0 : 1;
  if ($newKey) {
    ## This works for most data types, bar dominant
    if ( $outRef->{validKeys}{$type} eq "domwd" ) {
      my ( $val, $dir ) = processWindDirReading($value);
      $outRef->{data}{$binEndTime}{$type}{w}{$dir} += 1;
    } else {
      $outRef->{data}{$binEndTime}{$type} = $value;
    }
  } else {
    if ( $outRef->{validKeys}{$type} eq "avg" ) {
      $outRef->{data}{$binEndTime}{$type} += $value;
    } elsif ( $outRef->{validKeys}{$type} eq "min" ) {
      $outRef->{data}{$binEndTime}{$type} = $value if $outRef->{data}{$binEndTime}{$type} > $value;
    } elsif ( $outRef->{validKeys}{$type} eq "max" ) {
      $outRef->{data}{$binEndTime}{$type} = $value if $outRef->{data}{$binEndTime}{$type} < $value;
    } elsif ( $outRef->{validKeys}{$type} eq "domwd" ) {

      # This is domanant wind direction, increment counters for each direction
      my ( $val, $dir ) = processWindDirReading($value);
      $outRef->{data}{$binEndTime}{$type}{w}{$dir} += 1;
    }
  }
}

###############################################################################
# Split the headings into a hash keyed by their position.
# Subsequent data points can use this map to process the datapoint with the
# correct algorithm
###############################################################################
sub processingHeadings {
  my ( $headings, $resultsHashRef ) = @_;
  my @localheadArray = split m/,/, $headings;
  dateTimeLogging::windLogging::logOutInfo( 1, "HEADINGS: " . @localheadArray . " headngs in file\n" );
  for ( my $iter = 0 ; $iter < @localheadArray ; $iter++ ) {
    $resultsHash{hmap}{$iter} = @localheadArray[$iter];
    dateTimeLogging::windLogging::logOutInfo( 1, "HEADINGS: Col $iter is named as $resultsHash{hmap}{$iter}\n" );
  }
  return @localheadArray;
}

###############################################################################
# Setup the data processing hash. This is passed by reference to functions
# where data in added/processed. It holds some global control flags as well
# as the binning types foreach input heading. These must reflect the header
# names passed in the CSV
###############################################################################
sub initResultsSearchHash {
  my ($resultsHash) = @_;
  $resultsHash->{state}                = "searching";
  $resultsHash->{binendtime}           = 0;
  $resultsHash->{bincount}             = 0;
  $resultsHash->{lastHour}             = 0;
  $resultsHash->{hourSynced}           = -1;
  $resultsHash->{validProcType}{avg}   = "Normal average";
  $resultsHash->{validProcType}{min}   = "Keep min value";
  $resultsHash->{validProcType}{max}   = "Keep max value";
  $resultsHash->{validProcType}{domwd} = "most recurring value";
  $resultsHash->{validKeys}{bv}        = "avg";
  $resultsHash->{validKeys}{ws}        = "avg";
  $resultsHash->{validKeys}{wsi}       = "min";
  $resultsHash->{validKeys}{wsa}       = "max";
  $resultsHash->{validKeys}{it}        = "avg";
  $resultsHash->{validKeys}{wd}        = "domwd";
  $resultsHash->{cbin}                 = 0;                        # This will be the bin no
  $resultsHash->{hmap}                 = {};                       # Map headings to column
  $resultsHash->{data}                 = {};
}

###############################################################################
# initialise script configuration.
###############################################################################
sub initialiseConfigHash {
  my ($configHash) = @_;
  $$configHash{help}      = 0;                                     # print help
  $$configHash{verbose}   = 1;                                     # set the script verbose level, sets logging level
  $$configHash{startdate} = 0;                                     # start date
  $$configHash{enddate}   = getUniqueTimeStampSecDelay();          # end date
  $$configHash{inFile}    = "";                                    # Directory to read multiple input files from.
  $$configHash{outFile}   = "";                                    # Directory to read multiple input files from.
  $$configHash{minutes}   = 15;
}

###############################################################################
# Time manipulation
###############################################################################
sub getUniqueTimeStampSecDelay {
  my $dt = DateTime->now( time_zone => 'UTC' );
  return $dt->ymd('') . $dt->hms('');
}

sub getNextBinEndTime {
  my ( $timeString, $offsetInMins, $hourOnly ) = @_;
  my $time = 0;
  if ( $timeString =~ m/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/ ) {
    $time = DateTime->new(
      time_zone => 'UTC',
      year      => $1,
      month     => $2,
      day       => $3,
      hour      => $4,
      minute    => ( $hourOnly ? 0 : $5 ),
      second    => ( $hourOnly ? 0 : $6 )
    );
    $time->add( minutes => $offsetInMins );
    $time = $time->ymd('') . $time->hms('');
  } else {
    dateTimeLogging::windLogging::logOutInfo( 1, "Error time string is invalid: $time" );
  }
  return $time;
}

sub readFileContentsRetArrayRef {
  my ( $self, $fileName ) = @_;
  open( FILE, "$fileName" ) || die "Can't open $fileName: $!\n";
  my @temp = <FILE>;
  close(FILE);
  return \@temp;
}

sub writeToFile {
  my ( $self, $fileName, $string ) = @_;
  if ( $fileName ne "" ) {
    open( FILE, ">$fileName" ) || die "Can't open $fileName: $!\n";
    print FILE $string;
    close(FILE);
  }
}

###############################################################################
# print the License header!
###############################################################################
sub printLicenseHeader {
  print "###############################################################################
# dataFilter.pl v1.0, Copyright (C) 2015 thewindop.com
# dataFilter.pl comes with ABSOLUTELY NO WARRANTY; for details type `-h'.
###############################################################################
";
}

###############################################################################
# print the help text!
###############################################################################
sub printHelpText {

  my $helpText = <<EOTEXT;
  #############################################################################
  dataFilter.pl : Used to bin exported windop csv data.
  #############################################################################
  Copyright (C) 2015  thewindop.com

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

  Contact: andy\@thewindop.com  

  #############################################################################   
  All windop data is exported as CSV. This is the most flexible format.
  dataFilter.pl can further bin the data. Thewindop data has specific column 
  names that are fixed. This script uses these to select the appropiate type of 
  beheaviour when binning the data. For example wind speed is averaged, but the
  min max values are a single point extracted from the bin period.
  
  #############################################################################
  Current exported data headings.
  
  bv  = Average battery voltage. Currently only exported for thewindop.com use
  ws  = Average wind speed, historically this is measures over 40 seconds, 
        but can vary dependent on user settings
  wsi = Min wind speed, measured over 1 second
  wsa = Max wind speed, measured over 1 second
  it  = Internal temperature, temperature inside data sampler
  wd  = Wind direction 0 - 360 degrees
  
  #############################################################################
  Several time formats are added to ease use in excel as x series names
  ./dataFilter.pl -outdir ~/dataExpt/ -indir ./localDataProcess/scpFiles/ -device 36 -start 20151101000000 -remove 'bv'
  ./dataFilter.pl -inFile ~/dataExpt1/dev36/combined_dev36.csv -outFile ./temp_today.csv -minute 15

EOTEXT

  print $helpText . "\n";

}

package dateTimeLogging::windLogging;

use DateTime;

sub logOutWarn {
  my ( $level, $message ) = @_;
  return logOut( $level, " WARN: " . $message );
}

sub logOutInfo {
  my ( $level, $message ) = @_;
  return logOut( $level, " INFO: " . $message );
}

sub logOutMarker {
  my ( $level, $message ) = @_;
  return logOut( $level, " MARK: ========== : " . $message );
}

# This is an awesome routine! If you use this and your call is used on the server the log
# wrapper picks this message out and automatically forwards it via email at the end of the
# update sequence.
sub logOutError {
  my ( $level, $message ) = @_;
  return logOut( $level, " ERROR: " . $message );
}

sub logOutElevated {
  my ( $level, $message ) = @_;
  return logOut( $level, " ELEVATE: " . $message );
}

# finally we get to the base print routine. We have a global option here to print
# to a specified filename as well as to the screen so we get a debug log!
# We can add some clever shit here to create a new log every day or stuff like that!
sub printLogString {
  my ($string) = @_;
  my $logFile = "";

  # we use a global here to prevent too much messing around! lead by myGlobal__ which
  # should be unique at the top level script, which has a minimal package load!
  $logFile = $main::myGlobal__LoggingFileName if ( defined $main::myGlobal__LoggingFileName );

  # do the basic print
  print $string;

  # Do the append to the file
  if ( $logFile ne "" ) {
    open( FILE, ">>$logFile" ) || die "Can't open $logFile: $!\n";
    print FILE $string;
    close(FILE);
  }
  return $string;
}

sub logOut {
  my ( $level, $message ) = @_;
  my $logLevel = 0;
  my $string   = "";

  # we use a global here to prevent too much messing around! lead by myGlobal__ which
  # should be unique at the top level script, which has a minimal package load!
  $logLevel = $main::myGlobal__LoggingLevel if ( defined $main::myGlobal__LoggingLevel );

  if ( $logLevel >= $level ) {
    $string = "{" . getDateWithSlashsLog();
    $string .= " " . getTimeWithColonsLog();
    $string .= "}";
    $string .= $message;
    $string .= "\n";
    $string =~ s/[\n]*$/\n/;    # If there is no new line, or multiple, force to 1 new line!
    printLogString $string;
  }
  return $string;
}

# Use to print the output as is to the screen
sub logOutClean {
  my ( $level, $message ) = @_;
  my $logLevel = 0;

  # we use a global here to prevent too much messing around! lead by myGlobal__ which
  # should be unique at the top level script, which has a minimal package load!
  $logLevel = $main::myGlobal__LoggingLevel if ( defined $main::myGlobal__LoggingLevel );

  if ( $logLevel >= $level ) {
    $message =~ s/[\n]*$/\n/;    # If there is no new line, or multiple, force to 1 new line!
    printLogString $message;
  }
  return $message;
}

# define these 2 routines again or we have issues with a redefining of these routines.
# The log routine is bottom of the heap :)
sub getDateWithSlashsLog {

  $dt_jj = DateTime->now( time_zone => 'UTC' );
  return $dt_jj->ymd('/');       # 2002/12/06

}

sub getTimeWithColonsLog {
  $dt_jj = DateTime->now( time_zone => 'UTC' );
  return $dt_jj->hms(':');       # 14!02!29
}

1;

