

library(dplyr)
library(tidyr)
library(purrr)
library(forcats)
library(ggplot2)
library(ggridges)
library(lubridate)
library(DBI)
library(RPostgres)


# Parameters
end_date <- '2020-12-31' 

# Start date as string for query
start_date <- paste(
  year(as.Date(end_date) - years(2)), 
  sprintf('%02d', month(as.Date(end_date) %m-% months(3))), 
  '01', 
  sep = '-')


#==============================================================================
#
# Database connection and price data retrieval
#
#==============================================================================

# Connect to postgres database
con <- stock_master_connect()




# Read data
sql1 <- "select * from access_layer.return_attributes"
qry1 <- dbSendQuery(conn = con, statement = sql1) 
df_atts <- dbFetch(qry1)

sql2 <- "select *  from alpha_vantage.monthly_fwd_rtn where date_stamp >= ?start_date and date_stamp <= ?end_date"
sql2 <- sqlInterpolate(conn = con, sql = sql2, start_date = start_date, end_date = end_date)
qry2 <- dbSendQuery(conn = con, statement = sql2) 
df2_raw <- dbFetch(qry2)

df_filtered <- df_atts %>% 
  filter(date_stamp > as.Date('2011-12-31'), date_stamp < as.Date('2020-12-31')) %>% 
  group_by(symbol) %>% 
  mutate(fwd_rtn_1m = lead((adjusted_close-lag(adjusted_close))/lag(adjusted_close)),1) %>% 
  ungroup()


#==============================================================================
#
# Bar chart function
#
#==============================================================================

#' @param df A data frame - must contain a column 'fwd_rtn_1m' and a date at monthly intervals labelled 'date_stamp'
#' @param attribute The column representing the bars of bar chart
#' @param bins The discretisation level, either decile or quintile 
#' @param date_facet A logical specifying if faceting is to be performed.  If a year_filter is not selected, faceting is by year, else month
#' @param year_filter Integer, the year to filter for. If populated and date_facet = TRUE, faceting is by month

quantile_bar <- function(df, attribute, bins, date_facet, year_filter = NULL) {
  
  lookup <- setNames(
    as.list(
      c('12 month arithmetic return', '12 month arithmetic return by sector','3 month kurtosis of daily returns')), 
      c('rtn_ari_12m_dcl', 'rtn_ari_12m_sctr_dcl','kurt_ari_120d_dcl')
    )
  
  attribute_enquo <- enquo(attribute)
  attribute_name <- quo_name(attribute_enquo)
  bins <- enquo(bins)
  bins_name <- quo_name(bins)
  end_date <- max(df$date_stamp)
  
  # Logic for facet
  if (is.null(year_filter)){
    facet_var <- quo(year_stamp)
  } else {
    facet_var <- quo(date_stamp)
  }
  
  # Convert deciles to quintiles
  df <- df %>% mutate(
    quintile = case_when(
      !!attribute_enquo <= 2 ~ as.integer(1),
      !!attribute_enquo <= 4 ~ as.integer(2),
      !!attribute_enquo <= 6 ~ as.integer(3),
      !!attribute_enquo <= 8 ~ as.integer(4),
      TRUE                   ~ as.integer(5)
    ),
    attr_group_var = case_when(bins_name == 'decile' ~ !!attribute_enquo, TRUE ~ quintile),
    year_stamp = year(date_stamp)
  )
  
  # Plot if facet specified
  if (date_facet) {
    
  #  if (!is.null(year_filter)) {
  #    df <- 
  #  }
    
    
    df <- df %>%
      filter(
        if (is.null(year_filter)) {
          date_stamp < as.Date('9998-12-31')
        } else {
          year(date_stamp) == year_filter
        }
      )
    g <- df %>% 
      group_by(attr_group_var, !!facet_var) %>%
      summarise(fwd_rtn_1m = mean(fwd_rtn_1m, na.rm = TRUE)) %>%
      group_by(!!facet_var) %>%
      mutate(fwd_rtn_1m = scale(fwd_rtn_1m)) %>% 
      ggplot(aes(x = attr_group_var, y = fwd_rtn_1m)) +
      geom_col() + 
      facet_wrap(vars(!!facet_var), ncol = 3) +    # FACET
      theme_grey() +
      theme(
        plot.title    = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(face = "italic", size = 10),
        plot.caption  = element_text(face = "italic", size = 8),
        axis.title.y  = element_text(face = "italic", size = 9),
        axis.title.x  = element_text(face = "italic", size = 7),
        legend.position = "none"
      ) + 
      labs(title = paste('Monthly forward return by ',lookup[attribute_name], bins_name),
           subtitle = paste('Covering the period', min(df$date_stamp), 'to', max(df$date_stamp)),
           x = '', y = '')
    
    # Plot if facet NOT specified
  } else {
    df <- df %>%
      filter(
        if (is.null(year_filter)) {
          date_stamp < as.Date('9998-12-31')
        } else {
          year(date_stamp) == year_filter
        }
      )
    
    g <- df %>% 
      group_by(attr_group_var) %>%
      summarise(fwd_rtn_1m = mean(fwd_rtn_1m, na.rm = TRUE)) %>%
      mutate(fwd_rtn_1m = scale(fwd_rtn_1m)) %>% 
      ggplot(aes(x = attr_group_var, y = fwd_rtn_1m)) +
      geom_col() +
      theme_grey() +
      theme(
        plot.title    = element_text(face = "bold", size = 12),
        plot.subtitle = element_text(face = "italic", size = 10),
        plot.caption  = element_text(face = "italic", size = 8),
        axis.title.y  = element_text(face = "italic", size = 9),
        axis.title.x  = element_text(face = "italic", size = 7),
        legend.position = "none"
      ) + 
      labs(title = paste('Monthly forward return by ', lookup[attribute_name], bins_name),
           subtitle = paste('Covering the period', min(df$date_stamp), 'to', max(df$date_stamp)),
           x = '', y = '')
  }
  
  return(g)
}

quantile_bar(bn_data1, attribute = rtn_ari_6m_sctr_dcl, bins = quintile, date_facet = FALSE) # OK
quantile_bar(bn_data1, attribute = rtn_ari_6m_sctr_dcl, bins = quintile, date_facet = TRUE) # OK
quantile_bar(bn_data1, attribute = rtn_ari_6m_sctr_dcl, bins = quintile, date_facet = TRUE, year_filter = 2013) # OK
quantile_bar(bn_data1, attribute = rtn_ari_6m_sctr_dcl, bins = quintile, date_facet = TRUE, year_filter = 2014) # OK
quantile_bar(df_filtered, kurt_ari_120d_dcl, decile, TRUE) 


lookup <- setNames(
  as.list(c('12 month arithmetic return', '12 month arithmetic return by sector')), 
  c('rtn_ari_12m_dcl', 'rtn_ari_12m_sctr_dcl')
)

lookup$rtn_ari_12m_dcl
lookup['rtn_ari_12m_dcl']






#==============================================================================
#
# ggridges function
# https://cmdlinetips.com/2018/03/how-to-plot-ridgeline-plots-in-r/
#
#==============================================================================

quantile_ridges <- function(df, qntle) {
  
  qntle <- enquo(qntle)
  qntle_name <- quo_name(qntle)
  
  df <- df %>% drop_na() %>%
    select(date_stamp, !!qntle, fwd_rtn_1m) %>%
    filter(!!qntle %in% c(1, 10)) %>%
    mutate(decile_x = as.factor(!!qntle),
           date_stamp = fct_rev(as.factor(date_stamp)))
  
  ggp <- ggplot(df, aes(
    x = fwd_rtn_1m,
    y = date_stamp,
    fill = decile_x
  )) +
    geom_density_ridges(
      alpha = .6,
      color = 'white',
      from = -.6,
      to = .6,
      panel_scaling = FALSE
    ) +
    theme_grey() +
    theme(
      plot.title    = element_text(face = "bold", size = 12),
      plot.subtitle = element_text(face = "italic", size = 10),
      plot.caption  = element_text(face = "italic", size = 8),
      axis.title.y  = element_text(face = "italic", size = 9),
      axis.title.x  = element_text(face = "italic", size = 7),
      legend.position = "none"
    ) + 
    labs(title = paste(qntle_name, ' top and bottom quantile'),
         subtitle = 'this is the subtitle',
         x = '', y = '')
  
  return(ggp)
}

quantile_ridges(df_filtered, skew_ari_120d_sctr_dcl)


# ==================================================================================


ggplot(df_filtered, aes(x = as.factor(rtn_ari_12m_dcl), y = fwd_rtn_1m)) +
  geom_boxplot() +
  coord_flip()

# ==================================================================================



df_filtered %>% drop_na() %>%
  select(date_stamp, rtn_ari_12m_dcl, fwd_rtn_1m) %>%
  filter(rtn_ari_12m_dcl %in% c(1, 10)) %>%
  mutate(rtn_ari_12m_dcl = as.factor(rtn_ari_12m_dcl),
         date_stamp        = fct_rev(as.factor(date_stamp))) %>%
  ggplot(aes(
    x = fwd_rtn_1m,
    y = date_stamp,
    fill = rtn_ari_12m_dcl
  )) +
  geom_density_ridges(
    alpha = .6,
    color = 'white',
    from = -.6,
    to = .6,
    panel_scaling = FALSE
  )