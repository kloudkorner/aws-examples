package com.kloudkorner.aws.s3.examples;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.List;

public class Main
{
  public static void main(String[] args)
  {
    String accessKey = args[0];
    String secretKey = args[1];
    String bucketName = args[2];
    iterateAWSS3BucketKeys(accessKey, secretKey, bucketName);
  }

  public static void iterateAWSS3BucketKeys(String accessKey, String secretKey, String bucketName)
  {
    Regions clientRegion = Regions.US_EAST_1;
    try
    {
      final AWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
      final AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(basicAWSCredentials);
      final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
          .withCredentials(awsStaticCredentialsProvider)
          .withRegion(clientRegion)
          .build();

      final ListObjectsRequest listRequest = new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(5000);
      ObjectListing objects = s3Client.listObjects(listRequest);
      while (true)
      {
        final List<S3ObjectSummary> summaries = objects.getObjectSummaries();
        for (S3ObjectSummary summary : summaries)
        {
          String url = "https://" + summary.getBucketName() + ".s3.amazonaws.com/" + summary.getKey();
          System.out.println("url = " + url);
        }
        if (objects.isTruncated())
        {
          objects = s3Client.listNextBatchOfObjects(objects);
        }
        else
        {
          break;
        }
      }
    }
    catch (AmazonServiceException e)
    {
      e.printStackTrace();
    }
    catch (SdkClientException e)
    {
      e.printStackTrace();
    }
  }
}