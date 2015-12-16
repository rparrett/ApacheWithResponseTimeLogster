###  A logster parser file that collects statsd-style timing information
###  from an Apache access log that has been configured to log response
###  times
###
###  Based on https://github.com/etsy/statsd/blob/master/lib/process_metrics.js
###  and SampleLogster
###
###  rparrett
###

import time
import re
import math

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException

class ApacheWithResponseTimeLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.http_1xx = 0
        self.http_2xx = 0
        self.http_3xx = 0
        self.http_4xx = 0
        self.http_5xx = 0

        self.response_times = []

        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line (in this case, http_status_code, response_time).

        # LogFormat "%h %l %u %t \"%r\" %>s %b %D %{Content-type}o" responsetime
        self.reg = re.compile('.*HTTP/1.\d\" (?P<http_status_code>\d{3}) .*? (?P<response_time_us>\d+) (?P<content_type>.*?)$')


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        try:
            # Apply regular expression to each line and extract interesting bits.
            regMatch = self.reg.match(line)

            if regMatch:
                linebits = regMatch.groupdict()
                status = int(linebits['http_status_code'])

                if (status < 200):
                    self.http_1xx += 1
                elif (status < 300):
                    self.http_2xx += 1
                elif (status < 400):
                    self.http_3xx += 1
                elif (status < 500):
                    self.http_4xx += 1
                else:
                    self.http_5xx += 1

                if status == 200 and linebits['content_type'] == 'text/html':
                    self.response_times.append(float(linebits['response_time_us']) / 1000000)
            else:
                raise LogsterParsingException("regmatch failed to match")

        except Exception as e:
            raise LogsterParsingException("regmatch or contents failed with %s" % e)


    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        self.duration = float(duration)
        metricObjects = [
            MetricObject("http_1xx", (self.http_1xx / self.duration), "Responses per sec"),
            MetricObject("http_2xx", (self.http_2xx / self.duration), "Responses per sec"),
            MetricObject("http_3xx", (self.http_3xx / self.duration), "Responses per sec"),
            MetricObject("http_4xx", (self.http_4xx / self.duration), "Responses per sec"),
            MetricObject("http_5xx", (self.http_5xx / self.duration), "Responses per sec"),
        ]

        # response times

        self.response_times.sort()

        count = len(self.response_times)
        if count > 0:
            min = self.response_times[0]
            max = self.response_times[count-1]

            cumulativeValues = [min]
            cumulSumSquaresValues = [min * min]

            for i in range(1, count):
                cumulativeValues.append(self.response_times[i] + cumulativeValues[i-1])
                cumulSumSquaresValues.append((self.response_times[i] * self.response_times[i]) + cumulSumSquaresValues[i - 1])

            sum = min
            sumSquares = min * min
            mean = min
            thresholdBoundary = max

            thresholds = [-95, 95]
            for threshold in thresholds:
                numInThreshold = 0
                if count > 1:
                    numInThreshold = int(round(abs(threshold / 100.0 * count)))
                    print numInThreshold
                    if numInThreshold == 0:
                        continue

                    if threshold > 0:
                        thresholdBoundary = self.response_times[numInThreshold - 1]
                        sum = cumulativeValues[numInThreshold - 1]
                        sumSquares = cumulSumSquaresValues[numInThreshold - 1]
                    else:
                        thresholdBoundary = self.response_times[count - numInThreshold]
                        sum = cumulativeValues[count - 1] - cumulativeValues[count - numInThreshold - 1]
                        sumSquares = cumulSumSquaresValues[count - 1] - cumulSumSquaresValues[count - numInThreshold - 1]

                    mean = sum / numInThreshold

                clean_threshold = str(abs(threshold))
                upper_lower = "lower_"
                if threshold > 0:
                    upper_lower = "upper_"

                metricObjects.append(MetricObject("http_response_time.count_" + upper_lower + clean_threshold, numInThreshold))
                metricObjects.append(MetricObject("http_response_time.mean_" + upper_lower + clean_threshold, mean))
                metricObjects.append(MetricObject("http_response_time." + upper_lower + clean_threshold, thresholdBoundary))
                metricObjects.append(MetricObject("http_response_time." + upper_lower + "sum_" + clean_threshold, sum))
                metricObjects.append(MetricObject("http_response_time." + upper_lower + "sum_squares_" + clean_threshold, sumSquares))

            sum = cumulativeValues[count - 1]
            sumSquares = cumulSumSquaresValues[count - 1]
            mean = sum / count;

            mid = int(math.floor(count/2))
            median = 0
            if count % 2:
                median = self.response_times[mid]
            else:
                median = (self.response_times[mid - 1] + self.response_times[mid]) / 2

            sumOfDiffs = 0
            for i in range(0, count):
                sumOfDiffs = sumOfDiffs + (self.response_times[i] - mean) * (self.response_times[i] - mean);

            stdev = math.sqrt(sumOfDiffs / count)

            metricObjects.append(MetricObject("http_response_time.std", stdev))
            metricObjects.append(MetricObject("http_response_time.upper", max))
            metricObjects.append(MetricObject("http_response_time.lower", min))
            metricObjects.append(MetricObject("http_response_time.count", count))
            metricObjects.append(MetricObject("http_response_time.count_ps", count / self.duration))
            metricObjects.append(MetricObject("http_response_time.sum", sum))
            metricObjects.append(MetricObject("http_response_time.sum_squares", sumSquares))
            metricObjects.append(MetricObject("http_response_time.mean", mean))
            metricObjects.append(MetricObject("http_response_time.median", median))

        return metricObjects

