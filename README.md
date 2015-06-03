# fits-hilltop

This routine loads Hilltop formatted XML files generated from various Environmental monitoring systems and
passes them to FITS via an AWS SQS queue.

Example observations are of the form:

```
   <Measurement SiteName="Place">
   <DataSource Name="Air Temperature" NumItems="1">
   <Interpolation>Instantaneous</Interpolation>
   </DataSource>
   <Data DateFormat="UTC">
   <V> 27-May-15 21:30:00 2.300000</V>
   </Data>
```

The FITS method and network values are expected to be given on the command line, also given should be flags
linking the hilltop SiteName to the FITS station.

```
Usage of ./fits-hilltop:
  -dry-run=false: don't actually send the messages
  -key="": AWS access key id, overrides env and credentials file
  -method="": provide the FITS method
  -network="": provide the FITS network
  -queue="": send messages to the SQS queue, overides env variable "AWS_FITS_QUEUE"
  -region="": provide AWS region, overides env variable "AWS_FITS_REGION"
  -secret="": AWS secret key id, overrides env and credentials file
  -site=map[]: pass source name to site id conversions ["label"="code"]
  -verbose=false: make noise
```
