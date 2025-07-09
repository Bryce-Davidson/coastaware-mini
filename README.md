# Ocean Networks Canada Real-Time Data Streaming Examples

This repository contains Python examples for streaming real-time buoy data from Ocean Networks Canada (ONC) observatories.

## Overview

Ocean Networks Canada operates ocean observatories that continuously collect data from various sensors including:

- Wave height and period measurements
- Water temperature, salinity, and pressure
- Ocean currents
- Underwater acoustics
- And many more...

These scripts demonstrate how to access this data in real-time using the ONC API.

## Prerequisites

1. **ONC Account and API Token**

   - Register for a free account at https://data.oceannetworks.ca/Registration
   - Log in and go to your profile
   - Navigate to the "Web Services API" tab
   - Copy your API token

2. **Python Environment**
   - Python 3.8 or higher
   - Install dependencies: `pip install onc>=2.5.0`

## Setting Up

1. Set your API token as an environment variable:

   ```bash
   export ONC_TOKEN="your-token-here"
   ```

2. Install the project (using uv):

   ```bash
   uv sync
   ```

   Or with pip:

   ```bash
   pip install -r requirements.txt
   ```

## Examples

### 1. Basic Wave Height Streaming (`main.py`)

The original example that continuously streams wave height data from a specific buoy:

```bash
python main.py
```

Features:

- Fetches wave height data every 60 seconds
- Displays the last minute of measurements
- Automatic retry on errors
- Clean, formatted output

### 2. Simple Temperature Streaming (`simple_buoy_stream.py`)

A minimal example showing the basics of real-time data access:

```bash
python simple_buoy_stream.py
```

Features:

- Streams water temperature data
- Updates every 30 seconds
- Minimal code for easy understanding

### 3. Enhanced Multi-Sensor Streaming (`real_time_buoy_stream.py`)

An interactive example with multiple sensor types and customization options:

```bash
python real_time_buoy_stream.py
```

Features:

- Interactive location selection
- Multiple sensor types (wave, temperature, pressure, salinity)
- Configurable update intervals
- Color-coded terminal output
- Robust error handling

## Available Locations

Some popular ONC locations with real-time data:

- **SEVIP**: Strait of Georgia East
- **BARKLEY2**: Barkley Canyon Node 2
- **BCFN**: Folger Passage
- **USDDP**: Saanich Inlet
- **BACAX**: Barkley Canyon Axis

## Understanding the Data

### Time Series Data Structure

The ONC API returns time series data with two main arrays:

- `sampleTimes`: ISO 8601 timestamps for each measurement
- `values`: The actual sensor readings

### Common Sensor Types

1. **Wave Sensors (WAVSS)**

   - `sea_surface_wave_significant_height`: Wave height in meters
   - `sea_surface_wave_mean_period`: Wave period in seconds

2. **CTD Sensors** (Conductivity, Temperature, Depth)

   - `seawater_temperature`: Temperature in °C
   - `seawater_pressure`: Pressure in decibars
   - `salinity`: Salinity in PSU

3. **Current Profilers (ADCP)**
   - `eastward_sea_water_velocity`: Current speed eastward
   - `northward_sea_water_velocity`: Current speed northward

## API Usage Tips

1. **Rate Limiting**: Be respectful of the API. The examples use reasonable update intervals (30-60 seconds).

2. **Time Windows**: Request only the data you need. Smaller time windows return faster.

3. **Error Handling**: Network issues can occur. The examples include retry logic.

4. **Data Gaps**: Sensors may have occasional gaps in data. Check if values exist before processing.

## Exploring More Data

To discover more locations and sensors:

```python
from onc import ONC

onc = ONC("your-token")

# Find locations
locations = onc.getLocations()

# Find devices at a location
devices = onc.getDevices({"locationCode": "SEVIP"})

# Find properties for a device
properties = onc.getProperties({"deviceCode": "some-device-code"})
```

## Resources

- [ONC Data Portal](https://data.oceannetworks.ca)
- [API Documentation](https://wiki.oceannetworks.ca/display/O2A/Oceans+3.0+API+Home)
- [Python Client Documentation](https://oceannetworkscanada.github.io/api-python-client/)
- [Data Product Options](https://wiki.oceannetworks.ca/display/DP/Data+Product+Options)

## License

These examples are provided as-is for educational purposes. The ONC data is freely available under their data policy.
