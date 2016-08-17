# Cluster Autoscaler on AWS 
The cluster autoscaler on aws scales worker nodes within an autoscaling group. It will run as a `Deployment` in your cluster. This README will go over some of the necessary steps required to get the cluster autoscaler up and running. 

## Kubernetes Version 
Cluster autoscaler must run on v1.3.0 or greater. 

## Permissions 
You'll want to give worker nodes in your cluster access to these resources and actions, preferrably using IAM roles: 
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "autoscaling:DescribeAutoScalingGroups",
                "autoscaling:DescribeAutoScalingInstances",
                "autoscaling:SetDesiredCapacity",
                "autoscaling:TerminateInstanceInAutoScalingGroup"
            ],
            "Resource": "*"
        }
    ]
}
```
Unfortunately aws does not support ARNs for autoscaling groups yet so you must use "*" as the resource.

## Deployment Specification
Your deployment configuration should look something like this:
```
---
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: cluster-autoscaler
  labels:
    app: cluster-autoscaler
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cluster-autoscaler
  template:
    metadata:
      labels:
        app: cluster-autoscaler
    spec:
      containers:
        - image: {{ YOUR IMAGE HERE }}
          name: cluster-autoscaler
          resources:
            limits:
              cpu: 100m
              memory: 300Mi
            requests:
              cpu: 100m
              memory: 300Mi
          command:
            - ./cluster-autoscaler
            - -v=4
            - --cloud-provider=aws
            - --skip-nodes-with-local-storage=false
            - --nodes={{ ASG MIN e.g. 1 }}:{{ASG MAX e.g. 5}}:{{ASG NAME e.g. k8s-worker-asg}}
          env:
            - name: AWS_REGION
              value: us-east-1
          volumeMounts:
            - name: ssl-certs
              mountPath: /etc/ssl/certs/ca-certificates.crt
              readOnly: true
          imagePullPolicy: "Always"
      volumes:
        - name: ssl-certs
          hostPath:
            path: "/etc/ssl/certs/ca-certificates.crt"
```
Note: 
- the `/etc/ssl/certs/ca-certificates.crt` should exist by default on your ec2 instance. - cluster autoscaler is unaware of availability zones, the availability zone of the instance should be configured when the autoscaling group is created or in the launch configuration. 
- 
