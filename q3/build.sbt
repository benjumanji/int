libraryDependencies ++= Seq(
  "com.google.apis" % "google-api-services-storage" % "v1-rev47-1.20.0" exclude("com.google.gauva", "guava-jdk5"),
  "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.1.0",
  "joda-time" % "joda-time" % "2.8.2"
)
