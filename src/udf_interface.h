//
// Created by Ankush J on 11/24/18.
//

#pragma once

namespace {
    class udf_interface {
    public:
        udf_interface()
        virtual ~udf_interface()
        virtual void init() == 0;
        virtual void process() == 0;
        virtual void pause() == 0;
        virtual void resume() == 0;
        virtual void finalize() == 0;
    };
}

