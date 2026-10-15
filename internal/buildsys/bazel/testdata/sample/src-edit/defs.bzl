def _cat_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".txt")
    ctx.actions.run_shell(
        inputs = ctx.files.srcs,
        outputs = [out],
        command = "cat %s > %s" % (" ".join([f.path for f in ctx.files.srcs]), out.path),
    )
    return [DefaultInfo(files = depset([out]))]

cat = rule(implementation = _cat_impl, attrs = {"srcs": attr.label_list(allow_files = True)})

def _check_test_impl(ctx):
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(script, "#!/bin/sh\nexit 0\n", is_executable = True)
    return [DefaultInfo(executable = script, runfiles = ctx.runfiles(files = ctx.files.data))]

check_test = rule(implementation = _check_test_impl, test = True, attrs = {"data": attr.label_list(allow_files = True)})
