#!/usr/bin/perl -w
use strict;
use JSON;
use POSIX qw(strftime);
use Carp;
use LWP::Simple;
use Time::HiRes qw(usleep time);

use constant ATOMS_PER_DECRED => 10**8;

my $transactions = parse_json("dcrctl --wallet listtransactions '*' 1000 0 1");
my $balances = parse_json("dcrctl --wallet getbalance");
my %own_address = map { $_ => 1 } @{parse_json("dcrctl --wallet getaddressesbyaccount default")};
my %seen_txid = map { $_->{txid} => 1 } @$transactions;

printf("%-19s  %-10s  %-10s  %12s  %12s  %12s  %12s  %12s\n", "Block time", "Category", "TxType", "Amount", "Fee", "PoS reward", "Net reward", "Balance"); 
print "-" x 113 . "\n";
my $sum = 0;
my $balance = 0;
my $prevyear = -1;
my $open_ticketfees = 0;
my %open_tickets = ();
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
  if ($t->{txtype} eq "ticket" && ($t->{fee} || 0) != 0 && $t->{vout} == 0) {

    my $ticket_tx = get_rawtx($t->{blockhash}, $t->{txid});
    my $ticket_fees = 0;
    my $consolidating_tx_fees = 0;
    foreach my $vin (@{$ticket_tx->{vin}}) {
      $ticket_fees += $vin->{amountin};

      # check if the funding tx is consolidating our own inputs but we haven't seen it     
      $consolidating_tx_fees += traverse_unseen_own_txid_fees($vin->{blockheight}, $vin->{txid});
    }
    foreach my $vout (@{$ticket_tx->{vout}}) {
      $ticket_fees -= $vout->{value};
    }
    $ticket_fees = sprintf("%.8f", $ticket_fees);

    die "listtransactions ticket txfee $ticket_fees != ticket_tx fee $t->{fee}" if ($ticket_fees - abs($t->{fee}) > 0.000000001);

    $open_ticketfees += $ticket_fees + $consolidating_tx_fees;
    $open_tickets{$t->{txid}} = { ticket_tx => $ticket_tx, ticket_fees => $ticket_fees, consolidating_tx_fees => $consolidating_tx_fees };
    printf("%19s  %-10s  %-10s  %12.8f  %12.8f  %12s  %12s  %12s\n", strftime("%F %T", gmtime($t->{blocktime})), $t->{category}, $t->{txtype}, $t->{amount}, $t->{fee}, "-", "-", '...');
  } elsif ($t->{txtype} eq "vote" && $t->{amount} != 0) {
    my $tx = get_rawtx($t->{blockhash}, $t->{txid});

    # locate vin values
    my @vinValues = ();
    my $vinSum = 0;
    my ($ticket_blockheight, $ticket_txid);
    foreach my $vin (@{$tx->{vin}}) {
      $vinSum += $vin->{amountin};
      if ($vin->{amountin} > 0) {
        push @vinValues, $vin->{amountin};
      }
      if (exists $vin->{txid}) {
        $ticket_blockheight = $vin->{blockheight};
        $ticket_txid = $vin->{txid};
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

    # locate ticket tx
    die "ticket txid not found" if !defined $ticket_txid;
    die "ticket not found" if !exists $open_tickets{$ticket_txid};
    my $ticket = $open_tickets{$ticket_txid};
    delete $open_tickets{$ticket_txid};

    $open_ticketfees -= $ticket->{ticket_fees} + $ticket->{consolidating_tx_fees};

    #my $stakepool_fees = abs($t->{amount});
    #my $net_reward = $pos_reward - $stakepool_fees - $myticketfee - $ticket_fees;
    my $net_reward = $voutOwn - $vinTicket - $ticket->{ticket_fees} - $ticket->{consolidating_tx_fees};
    $sum += $net_reward * ATOMS_PER_DECRED;
    $balance += $net_reward * ATOMS_PER_DECRED;
    printf("%19s  %-10s  %-10s  %12.8f  %12.8f  %12.8f  %12.8f  %12.8f\n", strftime("%F %T", gmtime($t->{blocktime})), $t->{category}, $t->{txtype}, $voutSum, -$voutStakepool, $pos_reward, $net_reward, $balance / ATOMS_PER_DECRED);
  } elsif ($t->{txtype} eq "regular") {
    $balance += $t->{amount} * ATOMS_PER_DECRED;
    printf("%19s  %-10s  %-10s  %12.8f  %12.8f  %12s  %12s  %12.8f\n", strftime("%F %T", gmtime($t->{blocktime})), $t->{category}, $t->{txtype}, $t->{amount}, $t->{fee} || 0, "-", "-", $balance / ATOMS_PER_DECRED);
  }
}
print "-" x 113 . "\n";
if ($open_ticketfees != 0) {
  printf("%-86s %12.8f  %12.8f\n", "fees payed for live/immature tickets", -$open_ticketfees, -$open_ticketfees);
  $sum -= $open_ticketfees * ATOMS_PER_DECRED;
  $balance -= $open_ticketfees * ATOMS_PER_DECRED;
}
printf("%-86s %12.8f  %12.8f\n", "Total " . $prevyear, $sum / ATOMS_PER_DECRED, $balance / ATOMS_PER_DECRED);
print "=" x 113 . "\n";

printf("%-99s  %12.8f\n", "Balance", $balances->{cumulativetotal});
exit 0; 

# compare against mainnet.decred.org
my $last_query_start = 0;
foreach my $addr (keys %own_address) {
  my $last_query_interval = time() - $last_query_start; 
  if ($last_query_interval < 0.2) {
    usleep((0.2 - $last_query_interval) * 1000); # 5 requests per second max
  }
  $last_query_start = time();
  print("checking address $addr\n");
  my $txs = parse_json_http("https://mainnet.decred.org/api/txs/?address=$addr");
  foreach my $tx (@{$txs->{txs}}) {
    if (!$seen_txid{$tx->{txid}}) {
      print("address $addr: unseen txid $tx->{txid}\n");
    }
  }
}
exit 0; 

sub parse_json_http {
  my $url = shift;

  my $json = get($url);
  my $val = eval {
    return decode_json $json;
  };
  if ($@) {
    die "cannot parse JSON returned from $url: $@";
  }
  return $val;
}

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

my %block_cache = ();
sub get_block {
  my $blockhash = shift;

  if (exists $block_cache{$blockhash}) {
    return $block_cache{$blockhash};
  }

  my $block = parse_json("dcrctl getblock '$blockhash' 1 1");
  $block_cache{$blockhash} = $block;
  return $block;
}
 
sub get_rawtx {
  my $blockhash = shift;
  my $txid = shift;

  my $block = get_block($blockhash);
  foreach my $txcand (@{$block->{rawtx}}) {
    if (lc($txcand->{txid}) eq lc($txid)) {
      return $txcand;
    } 
  }
  foreach my $txcand (@{$block->{rawstx}}) {
    if (lc($txcand->{txid}) eq lc($txid)) {
      return $txcand;
    } 
  }
  croak "txid $txid not found in block $blockhash";
}

sub get_vin_address {
  my $vin = shift;

  if (exists $vin->{txid}) {
    my $tx = get_rawtx(get_blockhash($vin->{blockheight}), $vin->{txid});
    my $vin_vout = @{$tx->{vout}}[$vin->{vout}];
    if (exists $vin_vout->{scriptPubKey} && exists $vin_vout->{scriptPubKey}->{addresses}) {
      my @addresses = @{$vin_vout->{scriptPubKey}->{addresses}};
      die "multiple addresses for vin $vin" if (scalar @addresses > 1);
      if (scalar @addresses == 1) {
        return $addresses[0];
      }
    }
  }
  return undef;
}

sub traverse_unseen_own_txid_fees {
  my $blockheight = shift;
  my $txid = shift;

  my $consolidating_tx_fees = 0;
  if (!$seen_txid{$txid}) {
    my $funding_tx = get_rawtx(get_blockhash($blockheight), $txid);
    my $is_consolidating = 0;
    my $funding_tx_fees = 0;
    foreach my $funding_vin (@{$funding_tx->{vin}}) {
      my $funding_address = get_vin_address($funding_vin);
      if (defined $funding_address && $own_address{$funding_address}) {
        $is_consolidating = 1;
        $consolidating_tx_fees += traverse_unseen_own_txid_fees($funding_vin->{blockheight}, $funding_vin->{txid});
      }
      $funding_tx_fees += $funding_vin->{amountin};
    }
    foreach my $funding_vout (@{$funding_tx->{vout}}) {
      $funding_tx_fees -= $funding_vout->{value};
    }
    if ($is_consolidating) {
      $consolidating_tx_fees += $funding_tx_fees;
      printf("%19s  %-11s %-10s  %12s  %12.8f  %12s  %12s  %12s\n", strftime("%F %T", gmtime($funding_tx->{blocktime})), "consolidate", "ticket", "-", -$funding_tx_fees, "-", "-", '...');
    }
    $seen_txid{$txid} = 1;
  }
  return $consolidating_tx_fees;
}

my %blockhash_cache = ();
sub get_blockhash {
  my $blockheight = shift;

  if (exists $blockhash_cache{$blockheight}) {
    return $blockhash_cache{$blockheight};
  }

  my $hash = `dcrctl getblockhash $blockheight`;
  chomp($hash);
  $blockhash_cache{$blockheight} = $hash;

  return $hash;
}

