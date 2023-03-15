INCLUDE_DIRECTORIES(cereal/include)

SET(ULTRAVERSE_PROCASSIST_SRCS
    
    ProcCall.cpp
    ProcAssistPlugin.cpp
    ultraverse_procassist.cpp
    )

MYSQL_ADD_PLUGIN(ultraverse_procassist ${ULTRAVERSE_PROCASSIST_SRCS} RECOMPILE_FOR_EMBEDDED
  MODULE_ONLY MODULE_OUTPUT_NAME "ultraverse_procassist")
