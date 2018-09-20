# # documentation ----
# # File: 04Graphs.R
# # R Versions: x86_64
# # 
# # Author(s): AU
# # 
# # 
# # Description: Graphs from the summary of Tweets by day and similarity 
# #     between candidates from 03Analysis.py
# # 
# # Inputs: xlsx files with summary information
# # 
# # Outputs: png files with graphs
# # 
# # File history:
# #   20180220: creation

# path --------------------------------------------------------------------
inPath <- file.path("input")
outPath <- file.path("output")


# packages ----------------------------------------------------------------
library(tidyverse)  # # 1.2.1
library(lubridate)  # # 

library(openxlsx)  # # 4.0.17

# graph -------------------------------------------------------------------
datGraph <- read.xlsx(file.path(outPath, "counts.xlsx"))

datGraph2 <- read.xlsx(file.path(outPath, "counts2.xlsx"))

datGraph <- datGraph %>% 
  mutate(
    cand = ordered(candidato, 
                   levels = c('GP', 'SF', 'HC', 'GV', 'ID'),
                   labels = c('Petro', 'Fajardo', 'DeLaCalle', 'VargasLl', 'Duque')),
    date = ymd("2018-01-01") + weeks(week - 1)
    )


datGraph2 <- datGraph2 %>% 
  mutate(
    cand = ordered(candidato, 
                   levels = c('GP', 'SF', 'HC', 'GV', 'ID'),
                   labels = c('Petro', 'Fajardo', 'DeLaCalle', 'VargasLl', 'Duque')),
    date = as.Date(ISOdate(year, month, day))
  )

# # same colors as the wikipedia page of Colombian presidential elections
colores <- c('#8f0c9a', '#4bb9e7', '#c40016', '#9ebd3d', '#0062a2')[c(1, 5, 3, 2, 4)]

graphTw <- datGraph2 %>% 
  ggplot(aes(x = date, y = total / 1000, colour = cand)) +
  geom_line() + geom_point() +
  scale_color_manual(name = "", values = colores) +
  theme_bw() +
  labs(y = 'Thousand Tweets', x = '')

ggsave(file.path(outPath, "Total.png"), graphTw, width = 10, height = 7, units = "in") 

# # graph by topics
graphTw02 <- datGraph2 %>% 
  select(date, cand, amb:edu) %>% 
  gather('type', 'total', amb:edu) %>% 
  mutate(type = factor(type, labels = c('Environment', 'Economy', 'Education'))) %>% 
  ggplot(aes(x = date, y = total / 1000, colour = cand)) +
  geom_line() + geom_point() +
  scale_color_manual(name = '', values = colores) +
  theme_bw() +
  labs(y = 'Thousand Tweets', x = '') +
  facet_wrap(~type)

ggsave(file.path(outPath, "Topics.png"), graphTw02, width = 10, height = 7, units = "in")

# # heatmap from jaccard similarities
datGraph3 <- read.xlsx(file.path(inPath, "jaccard.xlsx"), sheet = "RT")

datGraph3 <- datGraph3 %>% 
  mutate(
    candL = ordered(cl, 
                   levels = c('GP', 'SF', 'HC', 'GV', 'ID'),
                   labels = c('Petro', 'Fajardo', 'DeLaCalle', 'VargasLl', 'Duque')),
    candD = ordered(cd, 
                    levels = c('GP', 'SF', 'HC', 'GV', 'ID'),
                    labels = c('Petro', 'Fajardo', 'DeLaCalle', 'VargasLl', 'Duque'))
  )

graphTw03 <- datGraph3 %>% #filter(type == 'RT_MEN') %>% 
  ggplot(aes(x = candL, y = candD, fill = jaccard)) +
  geom_tile(color = "white") +
  scale_fill_gradient(high = "#132B43", low = "#56B1F7", limit = c(0, 0.5),
                      space = "Lab", name = "Jaccard\nSimilarity") +
  theme_bw() +
  labs(y = '', x = '') +
  geom_text(aes(x = candL, y = candD, label = sprintf('%4.3f', jaccard))) +
  coord_fixed()

ggsave(file.path(outPath, "RT_Jaccard.png"), graphTw03, width = 10, height = 7, units = "in")

# #
datGraph4 <- read.xlsx(file.path(inPath, "jaccard.xlsx"), 
                       sheet = "MEN")


datGraph4 <- datGraph4 %>% 
  mutate(
    candL = ordered(cl, 
                    levels = c('GP', 'SF', 'HC', 'GV', 'ID'),
                    labels = c('Petro', 'Fajardo', 'DeLaCalle', 'VargasLl', 'Duque')),
    candD = ordered(cd, 
                    levels = c('GP', 'SF', 'HC', 'GV', 'ID'),
                    labels = c('Petro', 'Fajardo', 'DeLaCalle', 'VargasLl', 'Duque'))
  )


graphTw04 <- datGraph4 %>% filter(type == 'DIR') %>% 
  ggplot(aes(x = candL, y = candD, fill = jaccard)) +
  geom_tile(color = "white") +
  scale_fill_gradient(high = "#132B43", low = "#56B1F7", limit = c(0, 0.5),
                      space = "Lab", name = "Jaccard\nSimilarity") +
  theme_bw() +
  labs(y = '', x = '') +
  geom_text(aes(x = candL, y = candD, label = sprintf('%4.3f', jaccard))) +
  coord_fixed()

ggsave(file.path(outPath, "MEN_Jaccard.png"), graphTw04, width = 10, height = 7, units = "in")
