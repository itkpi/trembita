lazy val snapshot: Boolean = true
isSnapshot in ThisBuild := snapshot
version in ThisBuild := {
  val vv = "0.9.0-rc1"
  if (!snapshot) vv
  else vv + "-SNAPSHOT"
}