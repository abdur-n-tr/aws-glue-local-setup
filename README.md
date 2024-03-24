## AWS Glue Local Setup

- https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container

- To run a glue job locally, create a sample script like `first_glue_job.py` and run the below docker command by updating
the `mount paths` accordingly,

```bash
docker run -it -v ~/.aws:/home/glue_user/.aws -v /home/arehman/projects/setup-glue-locally/:/home/glue_user/workspace/ \
-e AWS_PROFILE=default -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 \
--name glue_spark_submit amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
spark-submit /home/glue_user/workspace/first_glue_job.py
```