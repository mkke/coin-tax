#!/usr/bin/perl
use JSON;
use POSIX qw(strftime);

use constant ATOMS_PER_DECRED => 10**8;

my $transactions = parse_json("dcrctl --wallet listtransactions '*' 1000");
my $balances = parse_json("dcrctl --wallet getbalance");

printf("%-19s  %-10s  %-10s  %12s  %12s  %12s  %12s  %12s\n", "Block time", "Category", "TxType", "Amount", "Fee", "PoS reward", "Net reward", "Balance"); 
print "-" x 113 . "\n";
my $sum = 0;
my $balance = 0;
my $prevyear = -1;
my @ticketfees = ();
foreach my $t (@$transactions) {
  my $year = (gmtime($t->{blocktime}))[5] + 1900;
  if ($year != $prevyear) {
    if ($prevyear >= 0) {
      print "-" x 113 . "\n";
      printf("%-86s %12.8f  %12.8f\n", "Total " . $prevyear, $sum / ATOMS_PER_DECRED, $balance / ATOMS_PER_DECRED);
      print "-" x 113 . "\n";
    }
    $prevyear = $year;
    $sum = 0;
  }
  if ($t->{txtype} eq "ticket" && $t->{fee} != 0 && $t->{vout} == 0) {
#    $sum -= abs($t->{fee} + $t->{amount}) * ATOMS_PER_DECRED;
#    $balance -= abs($t->{fee} + $t->{amount}) * ATOMS_PER_DECRED;
    push(@ticketfees, $t->{fee});
    printf("%19s  %-10s  %-10s  %12.8f  %12.8f  %12s  %12s  %12s\n", strftime("%F %T", gmtime($t->{blocktime})), $t->{category}, $t->{txtype}, $t->{amount}, $t->{fee}, "-", "-", '...');
  } elsif ($t->{txtype} eq "vote" && $t->{amount} != 0) {
    my $block = parse_json("dcrctl getblock '$t->{blockhash}' 1 1");
    my $tx = undef;
    foreach my $txcand (@{$block->{rawstx}}) {
      if (lc($txcand->{txid}) eq lc($t->{txid})) {
        $tx = $txcand;
        last;
      } 
    }
    die "txid $t->{txid} not found in block $t->{blockhash}" if !defined $tx;

    # locate vin values
    my @vinValues = ();
    my $vinSum = 0;
    foreach my $vin (@{$tx->{vin}}) {
      $vinSum += $vin->{amountin};
      if ($vin->{amountin} > 0) {
        push @vinValues, $vin->{amountin};
      }
    }
    die "2 vin values expected" if scalar @vinValues != 2;
    my ($vinTicket, $vinProfit);
    if ($vinValues[0] > $vinValues[1]) {
      $vinTicket = $vinValues[0];
      $vinProfit = $vinValues[1];
    } else {
      $vinProfit = $vinValues[0];
      $vinTicket = $vinValues[1];
    }

    # locate vout values
    my @voutValues = ();
    my $voutSum = 0;
    foreach my $vout (@{$tx->{vout}}) {
      $voutSum += $vout->{value};
      if ($vout->{value} > 0) {
        push @voutValues, $vout->{value};
      }
    }
    die "2 vout values expected" if scalar @voutValues != 2;
    my ($voutOwn, $voutStakepool);
    if ($voutValues[0] > $voutValues[1]) {
      $voutOwn = $voutValues[0];
      $voutStakepool = $voutValues[1];
    } else {
      $voutStakepool = $voutValues[0];
      $voutOwn = $voutValues[1];
    }
    my $lost = $vinTicket + $vinProfit - ($voutOwn + $voutStakepool);
    die "profit mismatch vinTicket = $vinTicket, vinProfit = $vinProfit, voutOwn = $voutOwn, voutStakepool = $voutStakepool" if ($lost < 0 || $lost > 1);

    #my $blocksubsidy = parse_json("dcrctl getblocksubsidy $block->{height} $block->{voters}");
    #my $pos_reward = (int($blocksubsidy->{pos}) / int($block->{voters}) - 1) / ATOMS_PER_DECRED; # value is off by one compared to blockexplorer, why? -> - 1
    my $pos_reward = $voutSum - $vinTicket;
    my $ticket_fees = abs(shift(@ticketfees));
    #my $stakepool_fees = abs($t->{amount});
    #my $net_reward = $pos_reward - $stakepool_fees - $myticketfee - $ticket_fees;
    my $net_reward = $voutOwn - $vinTicket - $ticket_fees;
    $sum += $net_reward * ATOMS_PER_DECRED;
    $balance += $net_reward * ATOMS_PER_DECRED;
    printf("%19s  %-10s  %-10s  %12.8f  %12.8f  %12.8f  %12.8f  %12.8f\n", strftime("%F %T", gmtime($t->{blocktime})), $t->{category}, $t->{txtype}, $voutSum, -$voutStakepool, $pos_reward, $net_reward, $balance / ATOMS_PER_DECRED);
  } elsif ($t->{txtype} eq "regular") {
    $balance += $t->{amount} * ATOMS_PER_DECRED;
    printf("%19s  %-10s  %-10s  %12.8f  %12.8f  %12s  %12s  %12.8f\n", strftime("%F %T", gmtime($t->{blocktime})), $t->{category}, $t->{txtype}, $t->{amount}, $t->{fee}, "-", "-", $balance / ATOMS_PER_DECRED);
  }
}
print "-" x 113 . "\n";
printf("%-86s %12.8f  %12.8f\n", "Total " . $prevyear, $sum / ATOMS_PER_DECRED, $balance / ATOMS_PER_DECRED);
if (@ticketfees > 0) {
  print "unaccounted ticketfees: @ticketfees\n";
}
print "=" x 113 . "\n";

printf("%-99s  %12.8f\n", "Balance", $balances->{cumulativetotal});

sub parse_json {
  my $cmd = shift;

  my $json;
  {
    local $/;
    open my $fh, $cmd . "|" or die "cannot pipe from $cmd: $!";
    $json = <$fh>;
  }
  my $val = eval {
    return decode_json $json;
  };
  if ($@) {
    die "cannot parse JSON returned for $cmd: $@";
  }
  return $val;
}

