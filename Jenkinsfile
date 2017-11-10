pipeline {
  agent any
  tools {
    jdk 'JDK 8u74'
  }
  stages {
    stage ('Build') {
      steps {
        withMaven(
                maven: '3.3.9',
                mavenSettingsFilePath: '/ct-data/oa/oap/maven-settings/settings.xml') {
          sh 'mvn -e -fae package'
        }
      }
      /* Handled by the Maven plugin?
      post {
        always {
          # String mangled for /* comment
          junit 'target/surefire-reports/** /*.xml'
        }
        success {
          # String mangled for /* comment
          archiveArtifacts artifacts: '** /target/*.jar', fingerprint: true
        }
      }
      */
    }
  }
}
