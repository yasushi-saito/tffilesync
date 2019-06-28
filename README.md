Syncer copies a remote directory to a another directory at the start.
It then continuously watches the latter directory and reflects any changes to it to the remote directory.

A typical usage is to sync TF checkpoint files to S3. Example:

```
syncer := Syncer(remote_dir='s3://mybucket/tfworkdir', local_dir='/tmp/tf')
.... syncer will automatically copy s3://mybucket/tfworkdir to /tmp/tf
.... run tensorflow using /tmp/tf as the workdir.
.... syncer will automatically sync changes to /tmp/tf to s3://mybucket/tfworkdir
syncer.stop() # call once all the computation is done
```
