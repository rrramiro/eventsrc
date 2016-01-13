/**
 * Atlassian Bamboo Plan template for eventsrc. Use this to create a build plan in Bamboo to build, test and release eventsrc.
 *
 * Linked repository to atlassianlabs/eventsrc repository needs to be configured with 'exclude changesets' set to the following
 * regex:
 *
 *   \[sbt-release\].*
 *   
 */

plan(key:'EVENTSRC',name:'eventsrc') {
   project(key:'OSSC',name:'Open Source Scala')
   
   repository(name:'eventsrc')
   
   trigger(type:'polling',description:'60 second chain trigger',
      strategy:'periodically',frequency:'60') {      
      repository(name:'eventsrc')
      
   }
   stage(name:'Build and test') {
      job(key:'SBT',name:'SBT') {         
         requirement(key:'sbt',condition:'exists')
         
         task(type:'checkout',description:'Checkout Default Repository')
         
         task(type:'script',description:'SBT',scriptBody:'''
#!/bin/bash
 
#https://extranet.atlassian.com/jira/browse/BUILDENG-2995
export JAVA_HOME=${bamboo.capability.system.jdk.JDK 1.8}
#https://extranet.atlassian.com/jira/browse/BUILDENG-7018
export SBT_OPTS="-Dsbt.log.noformat=true -J-XX:MaxPermSize=512M -sbt-dir /opt/bamboo-agent/.sbt -d"
./sbt clean +test -J-Xmx2G
''')

         task(type:'jUnitParser',description:'Parse test results',
              final:'true',resultsDirectory:'**/test-reports/*.xml')

      }
   }

   stage(name:'Release',description:'Release and publish artifacts',
           manual:'true') {
      job(key:'REL',name:'Release') {
         requirement(key:'sbt',condition:'exists')

         task(type:'checkout',description:'Checkout Default Repository') {
            repository(name:'eventsrc')
         }

         task(type:'script',description:'Set up remote tracking for push',
                 scriptBody:'''
#!/bin/bash

git remote set-url origin git@bitbucket.org:atlassianlabs/eventsrc.git
git fetch origin -v
git branch --set-upstream master origin/master

''')

         task(type:'script',description:'SBT',scriptBody:'''
#!/bin/bash

USER=$(fgrep "user=" ~/.ivy2/.credentials | cut -d= -f2)
PWD=$(fgrep "password=" ~/.ivy2/.credentials | cut -d= -f2)


#https://extranet.atlassian.com/jira/browse/BUILDENG-2995
export JAVA_HOME=${bamboo.capability.system.jdk.JDK 1.8}
#https://extranet.atlassian.com/jira/browse/BUILDENG-7018
export SBT_OPTS="-Dsbt.log.noformat=true -J-XX:MaxPermSize=512M -sbt-dir /opt/bamboo-agent/.sbt -d"

./sbt -Dsbt.log.noformat=true ";set credentials:=Seq(Credentials(\\"Sonatype Nexus Repository Manager\\", \\"maven.atlassian.com\\", \\"${USER}\\", \\"${PWD}\\")); release with-defaults" -J-Xmx2G
''')

         task(type:'jUnitParser',description:'Parse test results',
                 final:'true',resultsDirectory:'**/test-reports/*.xml')

      }
   }

   branchMonitoring(enabled:'true',timeOfInactivityInDays:'30',
      notificationStrategy:'NOTIFY_COMMITTERS',remoteJiraBranchLinkingEnabled:'true')
   
   dependencies(triggerOnlyAfterAllStagesGreen:'true',triggerForBranches:'true')
}

