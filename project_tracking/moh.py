import inspect
import logging

from .model import (
    FlagEnum
    )

def dna_bases_over_q30_percent_check(value):
    if int(value)<75:
        ret = FlagEnum("FAIL")
    elif int(value)<80:
        ret = FlagEnum("FLAG")
    else:
        ret = FlagEnum("PASS")
    return ret

def dna_aligned_reads_count_check(value, tumour):
    if int(value)<260000000 and not tumour:
        ret = FlagEnum("FAIL")
    elif int(value)<660000000 and not tumour:
        ret = FlagEnum("FLAG")
    elif int(value)<530000000 and tumour:
        ret = FlagEnum("FAIL")
    elif int(value)<1330000000 and tumour:
        ret = FlagEnum("FLAG")
    else:
        ret = FlagEnum("PASS")
    return ret

def dna_raw_mean_coverage_check(value, tumour):
    if float(value)<30 and not tumour:
        ret = FlagEnum("FAIL")
    elif float(value)<80 and tumour:
        ret = FlagEnum("FAIL")
    else:
        ret = FlagEnum("PASS")
    return ret

def rna_raw_reads_count_check(value):
    if int(value)<80000000:
        ret = FlagEnum("FAIL")
    elif int(value)<100000000:
        ret = FlagEnum("FLAG")
    else:
        ret = FlagEnum("PASS")
    return ret

def dna_raw_duplication_rate_check(value):
    if float(value)>50:
        ret = FlagEnum("FAIL")
    elif float(value)>20:
        ret = FlagEnum("FLAG")
    else:
        ret = FlagEnum("PASS")
    return ret

def median_insert_size_check(value):
    if float(value)<300:
        ret = FlagEnum("FLAG")
    elif float(value)<150:
        ret = FlagEnum("FAIL")
    else:
        ret = FlagEnum("PASS")
    return ret

def dna_contamination_check(value):
    if float(value)>5:
        ret = FlagEnum("FAIL")
    else:
        ret = FlagEnum("PASS")
    return ret

def dna_concordance_check(value):
    if float(value)<99:
        ret = FlagEnum("FAIL")
    else:
        ret = FlagEnum("PASS")
    return ret

def dna_tumour_purity_check(value):
    if float(value)<30:
        ret = FlagEnum("FAIL")
    else:
        ret = FlagEnum("PASS")
    return ret

def rna_exonic_rate_check(value):
    if float(value)<0.6:
        ret = FlagEnum("FAIL")
    elif float(value)<0.8:
        ret = FlagEnum("FLAG")
    else:
        ret = FlagEnum("PASS")
    return ret

def rna_ribosomal_contamination_count_check(value):
    if float(value)>0.35:
        ret = FlagEnum("FAIL")
    elif float(value)>0.1:
        ret = FlagEnum("FLAG")
    else:
        ret = FlagEnum("PASS")
    return ret

def rna_ribosomal_contamination_count_compute(rrna_count, rna_aligned_reads_count):
    return int(rrna_count)/int(rna_aligned_reads_count)
