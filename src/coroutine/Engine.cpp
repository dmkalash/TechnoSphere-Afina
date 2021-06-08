#include <afina/coroutine/Engine.h>


namespace Afina {
namespace Coroutine {

void Engine::Store(context &ctx) {

    char curr_pos;
    if (&curr_pos > ctx.Low) {
        ctx.High = (char *) &curr_pos;
    } else {
        ctx.Low = (char *) &curr_pos;
    }
    std::size_t size = ctx.High - ctx.Low;
    if (size > std::get<1>(ctx.Stack)) {
        delete[] std::get<0>(ctx.Stack);
        std::get<0>(ctx.Stack) = new char[size];
        std::get<1>(ctx.Stack) = size;
    }
    memcpy(std::get<0>(ctx.Stack), ctx.Low, size);
}

void Engine::Restore(context &ctx) {
    volatile char curr_pos;
    if (&curr_pos >= ctx.Low && &curr_pos <= ctx.High) {
        Restore(ctx);
    }
    memcpy(ctx.Low, std::get<0>(ctx.Stack), std::get<1>(ctx.Stack));
    longjmp(ctx.Environment, 1);
}

void Engine::yield() {
    context *calling = alive;
    while (calling == cur_routine && calling != nullptr) {
        calling = calling->next;
    }

    if (calling == nullptr) {
        return;
    }
    sched(calling);
}

void Engine::sched(void *routine_) {

    context *ctx = nullptr;
    if (routine_ == nullptr) {
        if (alive == nullptr) {
            return;
        }
        if (cur_routine == alive) {
            if (alive->next == nullptr) {
                return;
            }
            ctx = alive->next;
        } else {
            ctx = alive;
        }
    } else {
        ctx = static_cast<context *>(routine_);
    }

    if (ctx == cur_routine || ctx->is_blocked) {
        return;
    }

    if (cur_routine != idle_ctx) {
        if (setjmp(cur_routine->Environment) > 0) {
            return;
        }
        Store(*cur_routine);
    }
    Restore(*ctx);
}

void Engine::block(void *coroutine) {
    context* curr_coroutine = static_cast<context *>(coroutine);

    if (coroutine == nullptr) {
        curr_coroutine = cur_routine;
    }
    if (curr_coroutine->is_blocked) {
        return;
    }

    if (curr_coroutine->next != nullptr) {
        curr_coroutine->next->prev = curr_coroutine->prev;
    }
    if (curr_coroutine->prev != nullptr) {
        curr_coroutine->prev->next = curr_coroutine->next;
    }
    if (alive == curr_coroutine) {
        alive = alive->next;
    }

    curr_coroutine->is_blocked = true;
    curr_coroutine->next = blocked;
    curr_coroutine->prev = nullptr;
    if (blocked != nullptr) {
        blocked->prev = curr_coroutine;
    }
    blocked = curr_coroutine;
    if (coroutine == nullptr || coroutine == cur_routine) {
        Restore(*idle_ctx);
    }
}


void Engine::unblock(void* coroutine) {
    context* curr_coroutine = static_cast<context *>(coroutine);
    if (curr_coroutine == nullptr || !curr_coroutine->is_blocked) {
        return;
    }

    if (curr_coroutine->next != nullptr) {
        curr_coroutine->next->prev = curr_coroutine->prev;
    }
    if (curr_coroutine->prev != nullptr) {
        curr_coroutine->prev->next = curr_coroutine->next;
    }
    if (blocked == curr_coroutine) {
        blocked = blocked->next;
    }

    curr_coroutine->is_blocked = false;
    curr_coroutine->next = alive;
    curr_coroutine->prev = nullptr;
    if (alive != nullptr) {
        alive->prev = curr_coroutine;
    }
    alive = curr_coroutine;
}

} // namespace Coroutine
} // namespace Afina