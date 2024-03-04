"""
Module: validate_data
This module provides pytest tests for validating data.
"""
import pandas as pd
import pytest

def read_dataframes():
    """
    Reads data from csv files into pandas DataFrames.

    Returns:
    pandas.DataFrame: weather_df - DataFrame containing weather data.
    pandas.DataFrame: field_df - DataFrame containing field data.
    """
    weather_df = pd.read_csv('sampled_weather_df.csv')
    field_df = pd.read_csv('sampled_field_df.csv')
    return weather_df, field_df

def test_read_weather_DataFrame_shape():
    """
    Test 1: Checks if the weather DataFrame shape is as expected.
    """
    weather_df, _ = read_dataframes()
    assert weather_df.shape == (1834, 4)

def test_read_field_DataFrame_shape():
    """
    Test 2: Checks if the field DataFrame shape is as expected.
    """
    _, field_df = read_dataframes()
    assert field_df == (5654, 19)

def test_weather_DataFrame_columns():
    """
    Test 3: Checks if the columns in the weather DataFrame are as expected.
    """
    weather_df, _ = read_dataframes()
    expected_columns = ['Weather_station_ID', 'Message', 'Measurement', 'Value']
    assert all(col in weather_df.columns for col in expected_columns)

def test_field_DataFrame_columns():
    """
    Test 4: Checks if the columns in the field DataFrame are as expected.
    """
    _, field_df = read_dataframes()
    expected_columns = ['Field_ID', 'Elevation', 'Latitude', 'Longitude', 'Location', 'Slope',
                             'Rainfall', 'Min_temperature_C', 'Max_temperature_C', 'Ave_temps',
                             'Soil_fertility', 'Soil_type', 'pH', 'Pollution_level', 'Plot_size',
                             'Crop_type', 'Annual_yield', 'Standard_yield']
    assert all(col in field_df.columns for col in expected_columns)

def test_field_DataFrame_non_negative_elevation():
    """
    Test 5: Checks if elevation values in the field DataFrame are non-negative.
    """
    _, field_df = read_dataframes()
    assert all(field_df['Elevation'] >= 0)

def test_crop_types_are_valid():
    """
    Test 6: Checks if crop types in the field DataFrame are valid.
    """
    _, field_df = read_dataframes()
    valid_crop_types = ['cassava', 'wheat', 'tea', 'potato', 'banana', 'coffee', 'maize', 
                         'rice','cassava ','wheat ','tea ']
    assert all(crop.strip() in valid_crop_types for crop in field_df['Crop_type'].unique())

def test_positive_rainfall_values():
    """
    Test 7: Checks if rainfall values in the weather DataFrame are positive.
    """
    weather_df, _ = read_dataframes()
    assert all(weather_df['Rainfall'] >= 0)

