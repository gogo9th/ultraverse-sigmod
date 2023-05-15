INCLUDE_DIRECTORIES(cereal/include)
INCLUDE_DIRECTORIES(nlohmann-json/include)

SET(ULTRAVERSE_PROCASSIST_SRCS
    
    ProcCall.cpp
    ProcAssistPlugin.cpp
    ultraverse_procassist.cpp
    )


MYSQL_ADD_PLUGIN(ultraverse_procassist ${ULTRAVERSE_PROCASSIST_SRCS}
  MODULE_ONLY MODULE_OUTPUT_NAME "ultraverse_procassist")
