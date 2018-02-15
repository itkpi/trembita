import xerial.sbt.Sonatype._

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  "vitalii-honta",
  "Murcielago@as3341634df"
)

sonatypeProfileName := "com.github.vitaliihonta"

publishMavenStyle := true
pgpPassphrase := Some("as3341634df".toCharArray)
licenses := Seq("APL2" -> url("https://github.com/vitaliihonta/trembita/blob/master/LICENSE"))

homepage := Some(url("https://github.com/vitaliihonta/trembita"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/vitaliihonta/trembita"),
    "scm:git@github.com:vitaliihonta/trembita.git"
  )
)

developers := List(
  Developer(
    id = "vitliihonta",
    name = "Vitalii Honta",
    email = "murcielago13ua@gmail.com",
    url = url("https://github.com/vitaliihonta/")
  )
)

useGpg := false