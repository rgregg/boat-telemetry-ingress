import pytest
from agent.csvlp import annotated_csv_to_lp

def test_missing_datatype_annotation_raises():
    # Header + data row but NO #datatype annotation → cannot type fields → must raise
    # loudly rather than silently emitting bare (invalid) values for string/bool fields.
    csv = (
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,POWER_ON,value,m\r\n"
    )
    with pytest.raises(ValueError, match="datatype"):
        annotated_csv_to_lp(csv)

def test_double_field_with_tag():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement,host\r\n"
        ",_result,0,2026-01-01T00:00:00Z,2026-01-01T01:00:00Z,2026-06-14T18:40:00Z,1.5,load,cpu,a\r\n"
    )
    assert annotated_csv_to_lp(csv) == "cpu,host=a load=1.5 1781462400000000000"

def test_long_field_gets_i_suffix_and_escapes_tag():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement,room name\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,42,count,sensor,back cabin\r\n"
    )
    assert annotated_csv_to_lp(csv) == r"sensor,room\ name=back\ cabin count=42i 1781462400000000000"

def test_string_field_is_quoted():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ',_result,0,,,2026-06-14T18:40:00Z,under way,state,navigation\r\n'
    )
    assert annotated_csv_to_lp(csv) == 'navigation state="under way" 1781462400000000000'

def test_boolean_field():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,boolean,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,true,enabled,watchdog\r\n"
    )
    assert annotated_csv_to_lp(csv) == "watchdog enabled=true 1781462400000000000"

def test_empty_result_yields_empty_string():
    assert annotated_csv_to_lp("\r\n") == ""
    assert annotated_csv_to_lp("") == ""

def test_fractional_second_timestamp_is_exact():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ",_result,0,,,2026-06-14T18:40:00.500000Z,1.5,load,cpu\r\n"
    )
    assert annotated_csv_to_lp(csv) == "cpu load=1.5 1781462400500000000"

def test_field_key_with_space_is_escaped():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,1.5,mean value,cpu\r\n"
    )
    assert annotated_csv_to_lp(csv) == r"cpu mean\ value=1.5 1781462400000000000"

def test_empty_value_row_is_skipped():
    csv = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement\r\n"
        ",_result,0,,,2026-06-14T18:40:00Z,,load,cpu\r\n"
    )
    assert annotated_csv_to_lp(csv) == ""
