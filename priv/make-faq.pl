#!/usr/bin/perl

$input = shift;
$tmp1 = "/tmp/my-tmp.1.$$";
$tmp2 = "/tmp/my-tmp.2.$$";
$l1 = 0;
$l2 = 0;
$l3 = 0;

open(I, $input);
open(T1, "> $tmp1");
open(T2, "> $tmp2");

while (<I>) {
    if (/^##*/) {
        $line = $_;
        chomp;
        @a = split;
        $count = length($a[0]) - 2;
        if ($count >= 0) {
            if ($count == 0) {
                $l1++;
                $l2 = 0;
                $l3 = 0;
                $label = "$l1"
            }
            if ($count == 1) {
                $l2++;
                $l3 = 0;
                $label = "$l1.$l2"
            }
            if ($count == 2) {
                $l3++;
                $label = "$l1.$l2.$l3"
            }
            $indent = " " x ($count * 4);
            s/^#*\s*[0-9. ]*//;
            $anchor = "n$label";
            printf T1 "%s+ [%s. %s](#%s)\n", $indent, $label, $_, $anchor;
            printf T2 "<a name=\"%s\">\n", $anchor;
            $line =~ s/(#+)\s*[0-9. ]*/$1 $label.  /;
            print T2 $line;
        } else {
            print T2 $_, "\n";
        }
    } else {
        next if /^<a name="n[0-9.]+">/;
        print T2 $_;
    }
}

close(I);
close(T1);
close(T2);
open(T2, $tmp2);

while (<T2>) {
    if (/<!\-\- OUTLINE \-\->/) {
        print;
        print "\n";
        open(T1, $tmp1);
        while (<T1>) {
            print;
        }
        close(T1);
        while (<T2>) {
            if (/<!\-\- ENDOUTLINE \-\->/) {
                print "\n";
                print;
                last;
            }
        }
    } else {
        print;
    }
}
close(T2);

unlink($tmp1);
unlink($tmp2);
exit(0);
