# https://cran.r-project.org/doc/manuals/R-intro.html#Invoking-R-from-the-command-line
# https://stat.ethz.ch/R-manual/R-devel/library/utils/html/Rscript.html

args <- commandArgs(trailingOnly = TRUE)

#Arg 1
int_string <- args[1]
print('Argument 1 (integer string)')
print(c('Raw: ', int_string, ', Type: ', class(int_string)))

print("\n")

#Arg 2
date_string <- args[2]
print('Argument 1 (unquoted date string)')

print(c('Raw: ', date_string, ', Type: ', class(date_string)))
 
print("\n")

as_date <- as.Date(date_string)

print('Argument 2 (unquoted date string converted to date)')
print(c('Raw: ', as_date, ', Type: ', class(as_date)))

print("\n")

# Arg 3
char_string <- args[3]
print('Argument 3 (character string representing boolean)')
print(c('Raw: ', char_string, ', Type: ', class(char_string)))

print("\n")

char_string2log <- ifelse(tolower(char_string) == "t", TRUE, FALSE)

print('Argument 3 (character string converted to logicak)')
print(c('Raw: ', char_string2log, ', Type: ', class(char_string2log)))

# Grab latest commit SHA
git_sha <- substr(system("git rev-parse HEAD", intern=TRUE),1,10)

# Grab date
sys_date <- as.character(Sys.Date())

# Write all to csv
lst <- c(args[1], args[2], args[3], git_sha, sys_date)
write.csv(lst, file = 'C:/Users/brent/Documents/VS_Code/postgres/postgres/test/r_test.csv')
