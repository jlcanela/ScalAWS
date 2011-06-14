import sbt._
import Keys._

object ScalaAWSBuild extends Build{
  override lazy val settings = super.settings ++ (shellPrompt in ThisBuild := { s => Project.extract(s).currentProject.id + "> " })
  
  val awsVersion = "1.2.1"
  val specs2Version = "1.4"
  val jodaTimeVersion = "1.6.2"
  
  lazy val project = Project("library", file("library"))
    .settings(Defaults.defaultSettings : _*)
    .settings(name := "ScalAWS library", version := "0.1")
    .settings(resolvers := Seq(
      "snapshots" at "http://scala-tools.org/repo-snapshots"))
    .settings(libraryDependencies := Seq(
        "com.amazonaws" % "aws-java-sdk" % awsVersion,
        "joda-time" % "joda-time" % jodaTimeVersion,
        "org.specs2" %% "specs2" % specs2Version % "test"))
}