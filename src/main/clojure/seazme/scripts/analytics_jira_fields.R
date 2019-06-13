#
# one time
#
getwd()
setwd("seazme-tmpdir")
list.files()


#
# imports
#
library(dplyr)
library(ggplot2)
library(ramify)
library(plot3Drgl)

#
# load data
#
cfmap <-read.csv(gzfile("cfmap.csv"),header=F)
names(cfmap) <- c('customfield','customfieldname')

#manually remove .csv.gz
cfuse <-read.csv(gzfile("cfuse.csv"),header=F)
names(cfuse) <- c('customfield','max','count','variation')
cfuseexp<-full_join(cfuse,cfmap)
cfuse$max1<-NULL
cfuse$count1<-NULL


# WARNING: newer tickets are double counted (the updates might exist in many buckets), work around is a full scan, it statistically should not matter though.

agg2cf<-read.csv(gzfile("v.csv"),header=F)
names(agg2cf) <- c('week','project','customfield','count')
agg2p<-read.csv(gzfile("vp.csv"),header=F)
names(agg2p) <- c('week','project','count')


#
# const
#
week010119=626
week010118=574
week010117=521
week010116=469
tickets.total=sum(agg2p$count)


#
# project stats
#

p.stats <-agg2p %>% group_by(project) %>% summarise(p.count=sum(count),p.week.first=min(week),p.week.last=max(week),y19p=p.week.last>week010119,y18p=p.week.last>week010118,y17p=p.week.last>week010117)
ggplot(p.stats,aes(x=p.count)) + geom_histogram()
ggplot(p.stats,aes(x=log10(p.count))) + geom_histogram()

sum(agg2p$count)
sum(agg2p$count[agg2p$week>week010119])
sum(agg2p$count[agg2p$week>week010118])
sum(agg2p$count[agg2p$week>week010117])

nrow(p.stats)
length(which(p.stats$y19p))
length(which(p.stats$y18p))
length(which(p.stats$y17p))

sum(p.stats$p.count[which(p.stats$y19p)])
sum(p.stats$p.count[which(p.stats$y18p)])
sum(p.stats$p.count[which(p.stats$y17p)])

#
# week project graph
#
wp.stats <- full_join(p.stats,agg2p) %>%  group_by(week) %>% summarise(all=sum(count),current=sum(count[!y19p]))
ggplot(data=wp.stats) + geom_line(aes(x = week, y = all), color = "blue") + geom_line(aes(x = week, y = current), color = "red") + scale_x_continuous(breaks = seq(54, 1650, by = 52))


#
# customfields stats
#

cfstats <- function(w) {
     p.stats <-agg2p %>% filter(week>w) %>% group_by(project) %>% summarise(p.count=sum(count),p.week.first=min(week),p.week.last=max(week))
     #it is possible to derive agg2p from agg2p, the assumption is tht "created" field is in all tickets
     #cf.s1 <- agg2cf %>% group_by(project) %>% filter(customfield=="created") %>% summarise(project.count=sum(count))
     cf.s2 <- merge(agg2cf  %>% filter(week>w) %>% group_by(project,customfield) %>% summarise(c.count=sum(count),week.first=min(week), week.last=max(week)),p.stats)
     cf.s2 %>% mutate(use.ratio=c.count/p.count,use.per=sprintf("%s %d/%d or %1.2f%%",project,c.count,p.count,100*use.ratio))
}

cfstatsfull <- function(w) {
     cf.s3 <- cfstats(w)
     cf.s3 %>% group_by(customfield) %>% summarize(
                                     min.week.first=min(week.first),
                                     min.week.first=max(week.last),
                                     sum.c.count=sum(c.count),
                                     length.unique.project=length(unique(project)),
                                     weighted.mean.use.ratio=weighted.mean(use.ratio,p.count),
                                     project.stats=paste0(use.per, collapse = "; "))
}


cf.all <- cfstatsfull(-1)
cf.019 <- cfstatsfull(week010119)
cf.018 <- cfstatsfull(week010118)
cf.017 <- cfstatsfull(week010117)

# import into Excel, do not open it
write.csv(merge(x=cfuse, y=merge(cfmap,cf.all),by="customfield", all=TRUE),"stats-by-cf-all.csv")
write.csv(merge(x=cfuse, y=merge(cfmap,cf.019),by="customfield", all=TRUE),"stats-by-cf-019.csv")
write.csv(merge(x=cfuse, y=merge(cfmap,cf.018),by="customfield", all=TRUE),"stats-by-cf-018.csv")
write.csv(merge(x=cfuse, y=merge(cfmap,cf.017),by="customfield", all=TRUE),"stats-by-cf-017.csv")

ggplot(cf.019,aes(x=sum.c.count)) + geom_histogram()
ggplot(cf.all,aes(x=sum.c.count)) + geom_histogram()
ggplot(cf.all,aes(x=log10(sum.c.count))) + geom_histogram()


# density
cf2.all <- cfstats(-1)
cf2.all2<-cf2.all[with(cf.all,order(use.ratio)),]
ggplot(cf2.all2, aes(x = project, y = customfield)) + geom_tile(aes(fill = use.ratio)) + theme(axis.title.x=element_blank(),axis.text.x=element_blank(),axis.ticks.x=element_blank(), axis.title.y=element_blank(),axis.text.y=element_blank(),axis.ticks.y=element_blank())


#TODO: filter projects with low number of tickets
tmp1 <- agg2p %>% filter(week>620)
tmp2 <- tmp1 %>% group_by(project) %>% summarise(project.count=sum(count)) %>% filter(project.count> 50)


#
# 3D
#

#convert -delay 5 -loop 0 *.png do.gif


# projects by max recent update
mmdic <- agg2p  %>% group_by(project) %>% summarise(X = max(week))
mmdic2<-mmdic[order(mmdic$X),]
mmdic2$ID <- seq.int(nrow(mmdic2))
mmm=merge(x=agg2p, y=mmdic2)
scatter3Drgl(mmm$week,mmm$ID,clip(mmm$count,0,500),grid=TRUE,ticktype="detailed",cex=0.2)
play3d(spin3d(axis = c(0, 0, 1), rpm = 10), duration = 6.3)
movie3d(spin3d(axis = c(0, 0, 1)), duration = 15, dir="./tt",convert=FALSE)

# projects by max volume
mmdic <- aff2p  %>% group_by(project) %>% summarise(X = max(count))
mmdic2<-mmdic[order(mmdic$X, decreasing = TRUE),]
mmdic2$ID <- seq.int(nrow(mmdic2))
mmm=merge(x=agg2p, y=mmdic2)
scatter3Drgl(mmm$week,mmm$ID,clip(mmm$count,0,500),grid=TRUE,ticktype="detailed",cex=0.2)

# fields by max recent update
mm <- mmdic <- agg2cf %>% group_by(week,customfield) %>% summarise(count2 = sum(count))
mmdic <- mm %>% group_by(customfield) %>% summarise(X = max(week))
mmdic2<-mmdic[order(mmdic$X),]
mmdic2$ID <- seq.int(nrow(mmdic2))
mmm=merge(x=mm, y=mmdic2)
scatter3Drgl(mmm$week,mmm$ID,clip(mmm$count2,0,500),grid=TRUE,ticktype="detailed",cex=0.2)
play3d(spin3d(axis = c(0, 0, 1), rpm = 1), duration = 60)
movie3d(spin3d(axis = c(0, 0, 1), rpm= 2), duration = 40, dir="./tt",convert=FALSE)

# fields by max volume
mm <- mmdic <- agg2cf %>% group_by(week,customfield) %>% summarise(count2 = sum(count))
mmdic <- mm %>% group_by(customfield) %>% summarise(X = sum(count2))
mmdic2<-mmdic[order(mmdic$X, decreasing = TRUE),]
mmdic2$ID <- seq.int(nrow(mmdic2))
mmm=merge(x=m, y=mmdic2)
scatter3Drgl(mmm$week,mmm$ID,clip(mmm$count2,0,5000),grid=TRUE,ticktype="detailed",cex=0.2)
play3d(spin3d(axis = c(0, 0, 1), rpm = 1), duration = 60)
movie3d(spin3d(axis = c(0, 0, 1), rpm= 2), duration = 40, dir="./tt",convert=FALSE)

#
# scrap code
#

uv.qd$o <- order(uv.qd$project.count)
uv.qe <- uv.q1 %>% group_by(customfield) %>% summarise(customfield.count=sum(count))
uv.qe$oe <- order(uv.qe$customfield.count)
mm2 <- mm %>% group_by(V2) %>%  mutate(freqacc = cumsum(freq))

write.csv(merge(uv.q4,cfmap),"z.csv")
write.csv2(merge(uv.q4,cfmap),"z.csv",sep="\t")
write.table(merge(uv.q4,cfmap),"z.csv",sep="\t")
write.csv(merge(uv.q4,cfmap),"z.csv")
write.csv(cfmap,"f2.csv")
write.csv(cfmap2[1:],"f2.csv")

agg2pm=agg2p %>% group_by(m=week%/%10) %>% summarise(c=sum(count))
agg2pm=agg2p %>% group_by(project,m=week%/%10) %>% summarise(c=sum(count))
agg2pmc  <- agg2pm %>% arrange(m) %>% mutate(cs = cumsum(c))
ggplot(data=agg2pmc,mapping=aes(m,cs)) + geom_point()
