# Databricks notebook source
from chronicle import *

actual    = conform_column_name("HVERT ÅR ETTER HØST OG VINTER, KOMMER VÅREN IGJEN")
expected  = "hvert_ar_etter_host_og_vinter_kommer_varen_igjen"
assert expected == actual

actual    = conform_column_name(" | nord | sør | øst | vest | ")
expected  = "nord_sor_ost_vest"
assert expected == actual

actual    = conform_column_name("NordSør__ØstVest")
expected  = 'nord_sor_ost_vest'
assert expected == actual

actual    = conform_column_name("ØstOgVest")
expected  = 'ost_og_vest'
assert expected == actual

actual    = conform_column_name("ØSTLANDET")
expected  = 'ostlandet'
assert expected == actual

actual    = conform_column_name("ærlighet")
expected  = 'erlighet'
assert expected == actual

actual    = conform_column_name("Ærlighet")
expected  = 'erlighet'
assert expected == actual

actual    = conform_column_name("nothing_special")
expected  = 'nothing_special'
assert expected == actual

actual    = conform_column_name("A - little - Bit − strange")
expected  = 'a_little_bit_strange'
assert expected == actual

actual    = conform_column_name("PROVIDENCE")
expected  = 'providence'
assert expected == actual

actual    = conform_column_name("__providence__")
expected  = 'providence'
assert expected == actual
