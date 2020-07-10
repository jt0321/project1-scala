lazy val sparkVersion = "2.4.6"
lazy val akkaHttpVersion = "10.1.12"
lazy val akkaVersion = "2.6.6"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.github",
      scalaVersion := "2.12.11"
    )),
    name := "project1",
    version := "0.0.2",

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    //javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    javaOptions ++= Seq("-Xms2048M", "-Xmx2048M"),
    parallelExecution in Test := false,
    fork := true,
    cancelable in Global := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"                 % sparkVersion % Provided,

      "com.h2database" % "h2"                           % "1.4.192",
      "org.scalikejdbc" %% "scalikejdbc"                % "3.4.+",
      "org.scalikejdbc" %% "scalikejdbc-config"         % "3.4.+",
      "com.typesafe.akka" %% "akka-http"                % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-actor-typed"         % akkaVersion,
      "com.typesafe.akka" %% "akka-stream"              % akkaVersion,
      "ch.qos.logback"    % "logback-classic"           % "1.2.3",

      "org.scalikejdbc" %% "scalikejdbc-test"           % "3.4.+"         % Test,
      "org.scala-lang" % "scala-compiler"               % "2.12.11",
      "com.typesafe.akka" %% "akka-http-testkit"        % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion     % Test,
      "org.scalactic" %% "scalactic"                    % "3.2.0",
      "org.scalatest" %% "scalatest"                    % "3.2.0"         % Test
    ),

    initialCommands in console := """
      import org.apache.spark.sql.SparkSession
      import org.apache.spark.sql.functions._
      val spark = SparkSession.builder()
       .master("local[*]")
       .appName("spark-shell")
       .getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext
    """,

    cleanupCommands in console := "spark.stop()",
    coverageHighlighting := true,

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,
    runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

   resolvers ++= Seq(
      "Artima Maven Repository" at "https://repo.artima.com/releases/",
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    )
  )
