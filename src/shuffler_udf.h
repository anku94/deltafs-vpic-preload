//
// Created by Ankush J on 11/24/18.
//

#pragma once

#include "udf_interface.h"

namespace {
    class shuffler_udf : udf_interface {
        shuffler_udf();
        ~shuffler_udf();
        void init();
        void process();
        void pause();
        void resume();
        void finalize();
    };
}
