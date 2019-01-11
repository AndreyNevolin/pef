/**
 * Copyright © 2019 Andrey Nevolin, https://github.com/AndreyNevolin
 * Twitter: @Andrey_Nevolin
 * LinkedIn: https://www.linkedin.com/in/andrey-nevolin-76387328
 *
 * PEF internal time representation and time manipulation routines and macros
 *
 * The specifics of time measurement and time representation are isolated inside this
 * module so that different time measurement methods could be easily added without
 * affecting the code in other PEF modules
 */

#ifndef _PEF_TIME_H_
#define _PEF_TIME_H_

#include "common.h"

#define WTMLIB_ARCH_X86_64
#include "wtmlib.h"

/**
 * PEF time representation
 *
 * It is required that PEF time has high resolution. Not lower than 1 millisecond
 */
typedef uint64_t pef_time_t;

/**
 * "Zero" value of PEF time
 */
#define PEF_TIME_ZERO (uint64_t)0

/**
 * Maximum number of bytes that may be needed for storing PEF time as a null-terminated
 * string. The "null" symbol must be taken into account
 *
 * 21 is the number of bytes needed to store decimal representation of the maximum UINT64
 * value + the "null" symbol
 */
#define PEF_TIME_BUFF_SIZE 21

/**
 * Get current time
 *
 * NOTE: calls to "PEF_GETTIME()" should NOT be parts of wider statements. Each call to
 *       "PEF_GETTIME()" is expected to be a separate statement and take a separate line
 *       of the code:
 *       ........
 *       PEF_GETTIME( time_var);
 *       ........
 *
 *       That's because it's not specified what type the expression "PEF_GETTIME()" has
 *
 * Another possible implementation of the macro could be:
 *     clock_gettime( CLOCK_MONOTONIC_RAW, &_t)
 */
#define PEF_GETTIME( _t) _t = WTMLIB_GET_TSC()

/**
 * Convert seconds to PEF time format
 * (this conversion can potentially cause PEF time type overflow)
 *
 * "_conv_params" in this implementation is "TSC ticks per second"
 *
 * This macro doesn't fully isolate time conversion details to this module. The macro
 * requires time conversion parameters which can vary significantly depending on the exact
 * PEF time representation and conversion method used. Thus, when switching from one PEF
 * time representation to another it may be required to make changes not only to this
 * macro but also to all the places where the macro is used (to adapt to a new set of
 * conversion parameters). But at least this macro allows to find all such places easily
 */
#define PEF_SECS2PEF_TIME( _res, _secs, _conv_params)              \
    ({                                                             \
        PEF_ASSERT( UINT64_MAX / _conv_params >= (uint64_t)_secs); \
        _res = _secs * _conv_params;                               \
    })

/**
 * Divide time interval by an integer
 */
#define PEF_TIME_DIV( _res, _dividend, _divisor) \
    (                                            \
        _res = _dividend / _divisor              \
    )

/**
 * Subtract one time value from another
 */
#define PEF_TIME_SUB( _res, _minued, _subtrahend) \
    (                                             \
        _res = _minued - _subtrahend              \
    )

/**
 * Check whether one time period is less or equal to the other
 */
#define PEF_TIME_LEQ( _time1, _time2) \
    (                                 \
        _time1 <= _time2              \
    )

/**
 * Check whether one time period is strictly less than the other
 */
#define PEF_TIME_LESS( _time1, _time2) \
    (                                  \
        _time1 < _time2                \
    )

/**
 * Produce null-terminated string representation of time
 *
 * The buffer is expected to be of at least PEF_TIME_BUFF_SIZE bytes in size
 */
#define PEF_TIME2STR( _buff, _buff_size, _time)    \
    (                                              \
        snprintf( _buff, _buff_size, "%lu", _time) \
    )

/**
 * Convert PEF time to nanoseconds
 */
#define PEF_TIME2NSEC( _res, _time, _conv_params)                                 \
    ({                                                                            \
        uint64_t _nsecs_per_billion = 1000000000ll * 1000000000ll / _conv_params; \
        uint64_t _billion_worth = (_time / 1000000000) * _nsecs_per_billion;      \
        uint64_t _remainder = ((_time % 1000000000) * 1000000000) / _conv_params; \
                                                                                  \
        _res = _billion_worth + _remainder;                                       \
    })

#endif /* _PEF_TIME_H_ */
